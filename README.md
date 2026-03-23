# S&P 500 200-SMA Alert Screener

This script scans S&P 500 stocks, calculates the 200-day simple moving average, and alerts when a stock:

- touches the 200 SMA intraday,
- crosses it on the latest daily close, or
- closes within a configurable percentage of it.

## What it does

- Pulls the S&P 500 universe from Wikipedia by default
- Downloads daily bars from Alpaca
- Calculates the 200-day SMA locally
- Filters for names near the 200 SMA
- Prevents duplicate alerts with SQLite cooldown tracking
- Sends alerts to Discord and/or email
- Can export current matches to CSV

## Setup

1. Create and activate a virtual environment.
2. Install requirements:

```bash
pip install -r requirements.txt
```

3. Copy `.env.example` to `.env` or otherwise set the environment variables.

For macOS/Linux:

```bash
export ALPACA_KEY="your_key"
export ALPACA_SECRET="your_secret"
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

For Windows PowerShell:

```powershell
$env:ALPACA_KEY="your_key"
$env:ALPACA_SECRET="your_secret"
$env:DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
```

## Run locally

Dry run first:

```bash
python screener.py --dry-run --threshold 0.75 --output-csv candidates.csv
```

Normal run:

```bash
python screener.py --threshold 0.75 --cooldown-days 5
```

## Optional custom ticker file

If you want your own universe instead of the full S&P 500, create a CSV like this:

```csv
symbol,company
AAPL,Apple Inc.
MSFT,Microsoft Corp.
NVDA,NVIDIA Corp.
```

Then run:

```bash
python screener.py --tickers-csv my_tickers.csv --dry-run
```

## Suggested settings

A good starting setup is:

- threshold: `0.75`
- cooldown: `5`
- min price: `20`

## GitHub Actions automation

You can schedule this script to run automatically using the sample workflow in `.github/workflows/screener.yml`.

Add these GitHub repository secrets:

- `ALPACA_KEY`
- `ALPACA_SECRET`
- `ALPACA_FEED` (optional)
- `DISCORD_WEBHOOK_URL` (optional)
- `SMTP_HOST` (optional)
- `SMTP_PORT` (optional)
- `SMTP_USER` (optional)
- `SMTP_PASSWORD` (optional)
- `ALERT_EMAIL_FROM` (optional)
- `ALERT_EMAIL_TO` (optional)

## Notes

- This script is end-of-day / daily-bar oriented.
- If you want true intraday alerts the next upgrade is websocket streaming plus live SMA reference logic.
- Alpaca symbol handling can differ for a few special tickers. The script converts dash tickers to dot tickers automatically.
