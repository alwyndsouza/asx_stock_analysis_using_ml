"""
ASX Stock Data Extraction Pipeline using dlthub (Incremental).

Features:
- Automatic incremental loading based on date cursor
- State management handled by dlt
- Schema evolution and data quality checks
- Efficient upsert/merge operations

Usage:
    python -m ingestion.asx_extraction.extract run
    python -m ingestion.asx_extraction.extract run-full
    python -m ingestion.asx_extraction.extract run-incremental
    python -m ingestion.asx_extraction.extract schedule
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Iterator

import dlt
from dlt.sources.helpers import requests
import pandas as pd
import yfinance as yf

# Configuration
ASX_STOCKS = {
    "gold": ["NCM.AX", "EVN.AX", "NST.AX", "RRL.AX", "SBM.AX"],
    "oil": ["WDS.AX", "STO.AX", "BPT.AX", "KAR.AX"],
    "silver": ["SVL.AX", "S32.AX"],
}

DUCKDB_PATH = "asx_stocks.duckdb"
PIPELINE_NAME = "asx_stock_extraction"
DATASET_NAME = "raw_asx_data"

# Incremental settings
FULL_LOAD_YEARS = 5  # Fetch 5 years for full load
INCREMENTAL_DAYS = 30  # Fetch last 30 days for incremental

logger = logging.getLogger(__name__)


def get_sector(symbol: str) -> str:
    """Map symbol to sector."""
    for sector, symbols in ASX_STOCKS.items():
        if symbol in symbols:
            return sector
    return "unknown"


def get_all_symbols() -> list[str]:
    """Get all symbols from configuration."""
    return [sym for stocks in ASX_STOCKS.values() for sym in stocks]


@dlt.resource(
    name="asx_stock_prices",
    write_disposition="merge",
    primary_key=["date", "symbol"],
    merge_key=["date", "symbol"],
)
def fetch_stock_data_incremental(
    symbols: list[str] = None,
    start_date: str = None,
    incremental: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date",
        initial_value=None,
        allow_external_schedulers=True,
    ),
) -> Iterator[dict[str, Any]]:
    """
    Fetch ASX stock data with incremental loading support.
    
    Args:
        symbols: List of stock symbols to fetch (default: all configured stocks)
        start_date: Override start date (format: YYYY-MM-DD)
        incremental: dlt incremental state tracker
        
    Yields:
        Stock price records with OHLCV data
    """
    if symbols is None:
        symbols = get_all_symbols()
    
    # Determine date range
    if start_date:
        fetch_start_date = start_date
    elif incremental.last_value:
        # Incremental mode: fetch from last loaded date
        last_date = pd.to_datetime(incremental.last_value)
        fetch_start_date = (last_date - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"Incremental load from last value: {incremental.last_value}")
    else:
        # First run: fetch historical data
        fetch_start_date = (datetime.now() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
        logger.info(f"Initial load, fetching last {INCREMENTAL_DAYS} days")
    
    fetch_end_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"Fetching data from {fetch_start_date} to {fetch_end_date}")
    
    for symbol in symbols:
        logger.info(f"Fetching {symbol}")
        
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=fetch_start_date, end=fetch_end_date)
            
            if df.empty:
                logger.warning(f"No data available for {symbol}")
                continue
            
            df = df.reset_index()
            
            for _, row in df.iterrows():
                # Convert date to datetime without timezone
                date_val = row["Date"]
                if hasattr(date_val, "to_pydatetime"):
                    date_val = date_val.to_pydatetime()
                else:
                    date_val = pd.to_datetime(date_val).to_pydatetime()
                
                # Remove timezone info for consistency
                if date_val.tzinfo is not None:
                    date_val = date_val.replace(tzinfo=None)
                
                record = {
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
                
                yield record
            
            logger.info(f"Fetched {len(df)} records for {symbol}")
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            continue


@dlt.source(name="asx_stock_source")
def asx_stock_source(
    symbols: list[str] = None,
    start_date: str = None,
) -> Any:
    """
    dlt source for ASX stock data.
    
    Args:
        symbols: List of stock symbols to fetch
        start_date: Override start date (format: YYYY-MM-DD)
        
    Returns:
        dlt source with stock price resource
    """
    return fetch_stock_data_incremental(symbols=symbols, start_date=start_date)


def run_full() -> None:
    """Run full refresh - fetch 5 years of data using dlt pipeline."""
    logger.info(f"Starting FULL pipeline - fetching {FULL_LOAD_YEARS} years of data")
    
    start_date = (datetime.now() - timedelta(days=365 * FULL_LOAD_YEARS)).strftime("%Y-%m-%d")
    logger.info(f"Full load from {start_date} to today")
    
    # Create dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=DATASET_NAME,
        full_refresh=True,  # Force full refresh
    )
    
    # Load data
    load_info = pipeline.run(
        asx_stock_source(start_date=start_date),
        write_disposition="replace",  # Replace for full refresh
    )
    
    logger.info(f"Full refresh completed: {load_info}")
    logger.info(f"Loaded {load_info.load_packages[0].state.get('row_counts', {})} rows")


def run_incremental() -> None:
    """Run incremental - fetch recent data and merge using dlt's incremental loading."""
    logger.info("Starting INCREMENTAL pipeline")
    
    # Create dlt pipeline (reuses existing state)
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=DATASET_NAME,
        full_refresh=False,  # Preserve state for incremental
    )
    
    # Check if this is the first run
    try:
        state = pipeline.state
        has_state = bool(state.get("sources", {}).get("asx_stock_source"))
    except Exception:
        has_state = False
    
    if not has_state:
        logger.info("No existing state found. Running initial load...")
        # First run - load last 30 days
        start_date = (datetime.now() - timedelta(days=INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
        load_info = pipeline.run(
            asx_stock_source(start_date=start_date),
            write_disposition="merge",
        )
    else:
        logger.info("Incremental mode - loading new/updated records since last run")
        # Incremental run - dlt will track from last value
        load_info = pipeline.run(
            asx_stock_source(),
            write_disposition="merge",
        )
    
    logger.info(f"Incremental load completed: {load_info}")
    
    # Show pipeline state
    if load_info.load_packages:
        logger.info(f"Loaded rows: {load_info.load_packages[0].state.get('row_counts', {})}")


def run_pipeline() -> None:
    """Default: run incremental (safer for scheduled runs)."""
    return run_incremental()


def view_pipeline_info() -> None:
    """View information about the dlt pipeline state and loaded data."""
    logger.info("Viewing pipeline information")
    
    pipeline = dlt.pipeline(
        pipeline_name=PIPELINE_NAME,
        destination=dlt.destinations.duckdb(DUCKDB_PATH),
        dataset_name=DATASET_NAME,
    )
    
    try:
        # Show pipeline info
        logger.info(f"Pipeline name: {pipeline.pipeline_name}")
        logger.info(f"Dataset name: {pipeline.dataset_name}")
        logger.info(f"Destination: {pipeline.destination}")
        
        # Show state
        state = pipeline.state
        logger.info(f"Pipeline state: {state}")
        
        # Query record count
        with pipeline.sql_client() as client:
            with client.execute_query("SELECT COUNT(*) as count FROM asx_stock_prices") as cursor:
                result = cursor.fetchone()
                logger.info(f"Total records in asx_stock_prices: {result[0] if result else 0}")
            
            with client.execute_query("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT symbol) as symbol_count
                FROM asx_stock_prices
            """) as cursor:
                result = cursor.fetchone()
                if result:
                    logger.info(f"Date range: {result[0]} to {result[1]}")
                    logger.info(f"Unique symbols: {result[2]}")
    
    except Exception as e:
        logger.error(f"Error viewing pipeline info: {e}")


def schedule_pipeline():
    """Schedule the incremental pipeline to run daily using APScheduler."""
    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()

    scheduler.add_job(
        run_incremental,
        "cron",
        hour=6,
        minute=0,
        id="asx_incremental_load",
        name="ASX Incremental Data Load (dlt)",
        replace_existing=True,
    )

    logger.info("Scheduler started. Daily incremental loads at 6:00 AM (using dlt)")
    logger.info("Press Ctrl+C to stop")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ASX Stock Data Extraction Pipeline (dlthub)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run incremental load (default, merges new data)
  python -m ingestion.asx_extraction.extract run
  
  # Run full refresh (replaces all data)
  python -m ingestion.asx_extraction.extract run-full
  
  # Run incremental load explicitly
  python -m ingestion.asx_extraction.extract run-incremental
  
  # View pipeline state and data info
  python -m ingestion.asx_extraction.extract info
  
  # Schedule daily incremental loads
  python -m ingestion.asx_extraction.extract schedule
        """,
    )
    parser.add_argument(
        "command",
        choices=["run", "run-full", "run-incremental", "info", "schedule"],
        default="run",
        help="Command to run",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
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
    elif args.command == "info":
        view_pipeline_info()
    elif args.command == "schedule":
        schedule_pipeline()


if __name__ == "__main__":
    main()

    logger.info(f"Pipeline execution complete! Database: {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
