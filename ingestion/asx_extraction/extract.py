"""
ASX Stock Data Extraction Pipeline (Incremental).

Usage:
    python -m ingestion.asx_extraction.extract run
    python -m ingestion.asx_extraction.extract run-incremental
    python -m ingestion.asx_extraction.extract schedule
"""

import logging
import os
from datetime import datetime, timedelta

import duckdb
import pandas as pd
import yfinance as yf

# Configuration
ASX_STOCKS = {
    "gold": ["NCM.AX", "EVN.AX", "NST.AX", "RRL.AX", "SBM.AX"],
    "oil": ["WDS.AX", "STO.AX", "BPT.AX", "KAR.AX"],
    "silver": ["SVL.AX", "S32.AX"],
}

DUCKDB_PATH = "asx_stocks.duckdb"
DATASET_NAME = "raw_asx_data"

# Incremental settings
INCREMENTAL_DAYS = 30  # Fetch last 30 days for incremental

logger = logging.getLogger(__name__)


def get_sector(symbol: str) -> str:
    """Map symbol to sector."""
    for sector, symbols in ASX_STOCKS.items():
        if symbol in symbols:
            return sector
    return "unknown"


def fetch_stock_data(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch stock data for a single symbol from Yahoo Finance."""
    logger.info(f"Fetching {symbol} from {start_date} to {end_date}")
    records = []

    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date)

        if df.empty:
            logger.warning(f"No data available for {symbol}")
            return records

        df = df.reset_index()

        for _, row in df.iterrows():
            date_val = row["Date"]
            if hasattr(date_val, "to_pydatetime"):
                date_val = date_val.to_pydatetime()
            else:
                date_val = pd.to_datetime(date_val).to_pydatetime()

            records.append(
                {
                    "date": date_val,
                    "symbol": symbol,
                    "sector": get_sector(symbol),
                    "open_price": float(row["Open"]),
                    "high_price": float(row["High"]),
                    "low_price": float(row["Low"]),
                    "close_price": float(row["Close"]),
                    "volume": int(row["Volume"]),
                    "extraction_timestamp": datetime.now().isoformat(),
                }
            )

        logger.info(f"Fetched {len(records)} records for {symbol}")

    except Exception as e:
        logger.error(f"Error fetching {symbol}: {e}")

    return records


def load_to_duckdb(records: list[dict]):
    """Load records to DuckDB - always does full refresh for simplicity."""
    if not records:
        return

    os.makedirs(os.path.dirname(DUCKDB_PATH) or ".", exist_ok=True)

    con = duckdb.connect(DUCKDB_PATH)

    # Create schema if not exists
    con.execute("CREATE SCHEMA IF NOT EXISTS raw_asx_data")

    # Convert to DataFrame
    df = pd.DataFrame(records)

    # Convert date to datetime without timezone
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)

    # Define columns explicitly
    cols = [
        "date",
        "symbol",
        "sector",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "extraction_timestamp",
    ]
    df = df[cols]

    # Drop and recreate table (simplest approach for consistency)
    con.execute("DROP TABLE IF EXISTS raw_asx_data.asx_stock_prices")
    con.execute("CREATE TABLE raw_asx_data.asx_stock_prices AS SELECT * FROM df")

    count = con.execute("SELECT COUNT(*) FROM raw_asx_data.asx_stock_prices").fetchone()[0]
    logger.info(f"Loaded {len(records)} records. Total in DB: {count}")

    con.close()


def run_full() -> None:
    """Run full refresh - fetch 5 years of data."""
    logger.info("Starting FULL pipeline - fetching 5 years of data")

    start_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Fetching data from {start_date} to {end_date}")

    all_records = []
    all_symbols = [sym for stocks in ASX_STOCKS.values() for sym in stocks]

    for symbol in all_symbols:
        records = fetch_stock_data(symbol, start_date, end_date)
        all_records.extend(records)

    load_to_duckdb(all_records)
    logger.info("Full refresh completed")


def run_incremental() -> None:
    """Run incremental - fetch recent data and append (simplified: does full replace)."""
    logger.info("Starting INCREMENTAL pipeline")

    # Check if database exists
    if not os.path.exists(DUCKDB_PATH):
        logger.info("No database found. Running full instead.")
        return run_full()

    con = duckdb.connect(DUCKDB_PATH)
    try:
        result = con.execute("SELECT COUNT(*) FROM raw_asx_data.asx_stock_prices").fetchone()
        has_data = result and result[0] > 0
    except Exception:
        has_data = False
    finally:
        con.close()

    if not has_data:
        logger.info("No existing data found. Running full instead.")
        return run_full()

    # For simplicity, do incremental fetch with replace
    # This ensures we get the latest data for all days
    start_date = (datetime.now() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Fetching incremental data from {start_date} to {end_date}")

    all_records = []
    all_symbols = [sym for stocks in ASX_STOCKS.values() for sym in stocks]

    for symbol in all_symbols:
        records = fetch_stock_data(symbol, start_date, end_date)
        all_records.extend(records)

    if all_records:
        load_to_duckdb(all_records)
        logger.info("Incremental load completed")
    else:
        logger.info("No new data to load")


def run_pipeline() -> None:
    """Default: run full."""
    return run_full()


def schedule_pipeline():
    """Schedule the incremental pipeline to run daily."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()

    scheduler.add_job(
        run_incremental,
        "cron",
        hour=6,
        minute=0,
        id="asx_incremental_load",
        name="ASX Incremental Data Load",
        replace_existing=True,
    )

    logger.info("Scheduler started. Daily incremental loads at 6:00 AM")
    logger.info("Press Ctrl+C to stop")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="ASX Stock Data Extraction Pipeline")
    parser.add_argument(
        "command",
        choices=["run", "run-full", "run-incremental", "schedule"],
        default="run",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if args.command == "run":
        run_pipeline()
    elif args.command == "run-full":
        run_full()
    elif args.command == "run-incremental":
        run_incremental()
    elif args.command == "schedule":
        schedule_pipeline()

    logger.info(f"Pipeline execution complete! Database: {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
