from dotenv import load_dotenv
load_dotenv()

import argparse
import csv
import io
import os
import smtplib
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Iterable, List, Optional

import pandas as pd
import requests

try:
    import streamlit as st
except Exception:
    st = None

WIKI_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
DEFAULT_DB_PATH = "alerts.db"
DEFAULT_THRESHOLD_PCT = 0.75
DEFAULT_COOLDOWN_DAYS = 5
DEFAULT_LOOKBACK_DAYS = 420
DEFAULT_MIN_PRICE = 20.0
DEFAULT_MAX_TICKERS_PER_REQUEST = 150


@dataclass
class Alert:
    symbol: str
    company: str
    trigger_type: str
    close: float
    sma200: float
    distance_pct: float
    day_low: float
    day_high: float
    bar_time: str


class ScreenerError(Exception):
    pass


def env_required(name: str) -> str:
    value = os.getenv(name)

    if not value and st is not None:
        try:
            value = st.secrets[name]
        except Exception:
            pass

    if not value:
        raise ScreenerError(f"Missing required environment variable: {name}")
    return value


def load_sp500_tickers(csv_path: Optional[str] = None) -> pd.DataFrame:
    """Return DataFrame with columns: symbol, company."""
    if csv_path and os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        required = {"symbol", "company"}
        missing = required - set(df.columns.str.lower())
        # normalize column names if user used Symbol / Company
        colmap = {c: c.lower() for c in df.columns}
        df = df.rename(columns=colmap)
        if missing:
            raise ScreenerError(
                f"Ticker CSV must include columns {sorted(required)}. Found: {list(df.columns)}"
            )
        return df[["symbol", "company"]].copy()

    response = requests.get(WIKI_SP500_URL, timeout=30)
    response.raise_for_status()
    tables = pd.read_html(io.StringIO(response.text))
    if not tables:
        raise ScreenerError("Could not load S&P 500 table from Wikipedia.")

    df = tables[0][["Symbol", "Security"]].copy()
    df.columns = ["symbol", "company"]
    # Alpaca uses BRK.B and BF.B style dot tickers.
    df["symbol"] = df["symbol"].astype(str).str.replace("-", ".", regex=False)
    return df


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_alpaca_headers() -> dict:
    return {
        "APCA-API-KEY-ID": env_required("ALPACA_KEY"),
        "APCA-API-SECRET-KEY": env_required("ALPACA_SECRET"),
    }


def fetch_daily_bars(symbols: List[str], start: datetime, end: datetime) -> dict:
    headers = get_alpaca_headers()
    all_bars = {}

    for group in chunked(symbols, DEFAULT_MAX_TICKERS_PER_REQUEST):
        page_token = None
        while True:
            params = {
                "symbols": ",".join(group),
                "timeframe": "1Day",
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "limit": 10000,
                "adjustment": "all",
                "feed": os.getenv("ALPACA_FEED", "iex"),
                "sort": "asc",
            }
            if page_token:
                params["page_token"] = page_token

            response = requests.get(ALPACA_BARS_URL, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()

            for symbol, bars in payload.get("bars", {}).items():
                all_bars.setdefault(symbol, []).extend(bars)

            page_token = payload.get("next_page_token")
            if not page_token:
                break

    return all_bars


def build_price_df(raw_bars: List[dict]) -> pd.DataFrame:
    if not raw_bars:
        return pd.DataFrame()
    df = pd.DataFrame(raw_bars)
    rename_map = {"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns=rename_map)
    df["time"] = pd.to_datetime(df["time"], utc=True)
    df = df.sort_values("time").drop_duplicates(subset=["time"])
    df["sma200"] = df["close"].rolling(200).mean()
    return df


def determine_trigger(latest: pd.Series, previous: pd.Series, threshold_pct: float) -> Optional[str]:
    sma = latest["sma200"]
    if pd.isna(sma):
        return None

    close = latest["close"]
    low = latest["low"]
    high = latest["high"]
    distance_pct = abs(close - sma) / sma * 100

    if previous is not None and not pd.isna(previous["sma200"]):
        prev_close = previous["close"]
        prev_sma = previous["sma200"]
        if prev_close > prev_sma and close <= sma:
            return "cross_below_or_touch"
        if prev_close < prev_sma and close >= sma:
            return "cross_above_or_touch"

    if low <= sma <= high:
        return "intraday_touch"

    if distance_pct <= threshold_pct:
        return "near_200sma"

    return None


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                trigger_type TEXT NOT NULL,
                bar_date TEXT NOT NULL,
                alerted_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def was_alerted_recently(db_path: str, symbol: str, cooldown_days: int) -> bool:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=cooldown_days)).isoformat()
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM alert_history
            WHERE symbol = ? AND alerted_at >= ?
            ORDER BY alerted_at DESC
            LIMIT 1
            """,
            (symbol, cutoff),
        ).fetchone()
        return row is not None


def record_alerts(db_path: str, alerts: List[Alert]) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO alert_history(symbol, trigger_type, bar_date, alerted_at)
            VALUES(?, ?, ?, ?)
            """,
            [(a.symbol, a.trigger_type, a.bar_time, now) for a in alerts],
        )
        conn.commit()


def format_alert_message(alerts: List[Alert]) -> str:
    lines = [f"S&P 500 names near the 200 SMA: {len(alerts)}"]
    for a in alerts:
        lines.append(
            f"- {a.symbol} ({a.company}) | {a.trigger_type} | close {a.close:.2f} | "
            f"SMA200 {a.sma200:.2f} | dist {a.distance_pct:.2f}% | low/high {a.day_low:.2f}/{a.day_high:.2f} | {a.bar_time}"
        )
    return "\n".join(lines)


def send_discord(message: str) -> None:
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return
    response = requests.post(webhook_url, json={"content": message[:1900]}, timeout=30)
    response.raise_for_status()


def send_email(message: str) -> None:
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_password = os.getenv("SMTP_PASSWORD")
    to_email = os.getenv("ALERT_EMAIL_TO")
    from_email = os.getenv("ALERT_EMAIL_FROM", smtp_user)

    if not all([smtp_host, smtp_port, smtp_user, smtp_password, to_email, from_email]):
        return

    msg = EmailMessage()
    msg["Subject"] = "S&P 500 200 SMA Alerts"
    msg["From"] = from_email
    msg["To"] = to_email
    msg.set_content(message)

    with smtplib.SMTP(smtp_host, int(smtp_port), timeout=30) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def save_csv(alerts: List[Alert], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "symbol",
            "company",
            "trigger_type",
            "close",
            "sma200",
            "distance_pct",
            "day_low",
            "day_high",
            "bar_time",
        ])
        for a in alerts:
            writer.writerow([
                a.symbol,
                a.company,
                a.trigger_type,
                f"{a.close:.2f}",
                f"{a.sma200:.2f}",
                f"{a.distance_pct:.2f}",
                f"{a.day_low:.2f}",
                f"{a.day_high:.2f}",
                a.bar_time,
            ])


def run_screener(
    threshold_pct: float,
    cooldown_days: int,
    csv_path: Optional[str],
    db_path: str,
    min_price: float,
    dry_run: bool,
    output_csv: Optional[str],
) -> List[Alert]:
    init_db(db_path)
    universe = load_sp500_tickers(csv_path)
    symbols = universe["symbol"].tolist()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=DEFAULT_LOOKBACK_DAYS)
    bars_map = fetch_daily_bars(symbols, start, end)

    alerts: List[Alert] = []
    company_lookup = dict(zip(universe["symbol"], universe["company"]))

    for symbol in symbols:
        raw_bars = bars_map.get(symbol, [])
        df = build_price_df(raw_bars)
        if len(df) < 200:
            continue

        latest = df.iloc[-1]
        previous = df.iloc[-2] if len(df) >= 2 else None
        if pd.isna(latest["sma200"]):
            continue
        if latest["close"] < min_price:
            continue

        trigger = determine_trigger(latest, previous, threshold_pct)
        if not trigger:
            continue
        if was_alerted_recently(db_path, symbol, cooldown_days):
            continue

        distance_pct = abs(latest["close"] - latest["sma200"]) / latest["sma200"] * 100
        alerts.append(
            Alert(
                symbol=symbol,
                company=company_lookup.get(symbol, symbol),
                trigger_type=trigger,
                close=float(latest["close"]),
                sma200=float(latest["sma200"]),
                distance_pct=float(distance_pct),
                day_low=float(latest["low"]),
                day_high=float(latest["high"]),
                bar_time=latest["time"].strftime("%Y-%m-%d"),
            )
        )

    alerts.sort(key=lambda x: (x.distance_pct, x.symbol))

    if output_csv:
        save_csv(alerts, output_csv)

    message = format_alert_message(alerts) if alerts else "No S&P 500 names triggered today."
    print(message)

    if alerts and not dry_run:
        send_discord(message)
        send_email(message)
        record_alerts(db_path, alerts)

    return alerts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alert when S&P 500 stocks are near their 200-day SMA.")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD_PCT, help="Percent distance from SMA200 to alert on. Default: 0.75")
    parser.add_argument("--cooldown-days", type=int, default=DEFAULT_COOLDOWN_DAYS, help="Suppress repeat alerts for N days per symbol.")
    parser.add_argument("--tickers-csv", type=str, default=None, help="Optional CSV file with columns: symbol,company")
    parser.add_argument("--db", type=str, default=DEFAULT_DB_PATH, help="SQLite DB path for alert history.")
    parser.add_argument("--min-price", type=float, default=DEFAULT_MIN_PRICE, help="Ignore stocks below this close price.")
    parser.add_argument("--dry-run", action="store_true", help="Print matches without sending alerts or recording history.")
    parser.add_argument("--output-csv", type=str, default=None, help="Optional output CSV path for current matches.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        run_screener(
            threshold_pct=args.threshold,
            cooldown_days=args.cooldown_days,
            csv_path=args.tickers_csv,
            db_path=args.db,
            min_price=args.min_price,
            dry_run=args.dry_run,
            output_csv=args.output_csv,
        )
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
