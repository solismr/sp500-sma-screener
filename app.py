import streamlit as st
import pandas as pd
from screener import run_screener

st.set_page_config(page_title="S&P 500 200 SMA Screener", layout="wide")

st.title("📊 S&P 500 — 200 SMA Screener")

threshold = st.slider("Distance Threshold (%)", 0.1, 2.0, 0.75, 0.05)
min_price = st.number_input("Minimum Stock Price", value=20.0)

if st.button("Run Scan"):
    with st.spinner("Scanning market..."):

        alerts = run_screener(
            threshold_pct=threshold,
            cooldown_days=5,
            csv_path="sp500_tickers.csv",
            db_path="alerts.db",
            min_price=min_price,
            dry_run=True,
            output_csv=None,
        )

    if alerts:
        df = pd.DataFrame([a.__dict__ for a in alerts])
        df = df.sort_values("distance_pct")

        st.success(f"{len(df)} stocks near 200 SMA")
        st.dataframe(df, use_container_width=True)

    else:
        st.warning("No stocks triggered today")