"""
Database utilities for Streamlit apps.

Handles automatic download of the latest DuckDB from GitHub Releases
if the database doesn't exist locally.
"""

import logging
import streamlit as st
from pathlib import Path
import sys

# Add scripts directory to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

logger = logging.getLogger(__name__)


@st.cache_resource(ttl=3600)  # Cache for 1 hour
def ensure_database_exists():
    """
    Ensure the DuckDB database exists locally.
    
    If not found, downloads the latest version from GitHub Releases.
    
    Returns:
        Path: Path to the DuckDB file
        
    Raises:
        FileNotFoundError: If database cannot be found or downloaded
    """
    db_path = PROJECT_ROOT / "asx_stocks.duckdb"
    
    # Check if database exists
    if db_path.exists():
        file_size_mb = db_path.stat().st_size / (1024 * 1024)
        st.info(f"📊 Using local database ({file_size_mb:.1f} MB)")
        return db_path
    
    # Database doesn't exist - try to download
    st.warning("⏳ Database not found locally. Downloading latest from GitHub...")
    
    try:
        from download_latest_db import download_latest_database
        
        with st.spinner("Downloading database from GitHub Releases..."):
            success = download_latest_database(force=False)
        
        if success and db_path.exists():
            st.success("✅ Database downloaded successfully!")
            return db_path
        else:
            st.error("❌ Failed to download database")
            raise FileNotFoundError(
                "Could not download database. Please run: "
                "python scripts/download_latest_db.py"
            )
    
    except ImportError:
        st.error("❌ Download script not found")
        raise FileNotFoundError(
            "Database not found and cannot download. "
            "Please run: python scripts/download_latest_db.py"
        )


def get_database_connection(read_only: bool = True):
    """
    Get a DuckDB connection to the ASX stocks database.
    
    Args:
        read_only: Whether to open in read-only mode (default: True)
        
    Returns:
        duckdb.Connection: Database connection
    """
    import duckdb
    
    db_path = ensure_database_exists()
    
    try:
        con = duckdb.connect(str(db_path), read_only=read_only)
        return con
    except Exception as e:
        st.error(f"❌ Failed to connect to database: {e}")
        raise


def get_database_info():
    """
    Get information about the database.
    
    Returns:
        dict: Database statistics
    """
    con = get_database_connection()
    
    try:
        # Get record counts
        raw_count = con.execute(
            "SELECT COUNT(*) FROM raw_asx_data.asx_stock_prices"
        ).fetchone()[0]
        
        mart_count = con.execute(
            "SELECT COUNT(*) FROM analytics.mart_ml_training_dataset"
        ).fetchone()[0]
        
        # Get date range
        date_info = con.execute(
            """
            SELECT 
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(DISTINCT symbol) as symbol_count
            FROM raw_asx_data.asx_stock_prices
            """
        ).fetchone()
        
        return {
            "raw_records": raw_count,
            "mart_records": mart_count,
            "min_date": date_info[0],
            "max_date": date_info[1],
            "symbol_count": date_info[2],
        }
    
    finally:
        con.close()


def display_database_info():
    """Display database information in Streamlit sidebar."""
    try:
        info = get_database_info()
        
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📊 Database Info")
        st.sidebar.metric("Raw Records", f"{info['raw_records']:,}")
        st.sidebar.metric("ML Dataset", f"{info['mart_records']:,}")
        st.sidebar.metric("Symbols", info['symbol_count'])
        st.sidebar.caption(
            f"Data: {info['min_date']} to {info['max_date']}"
        )
        
    except Exception as e:
        st.sidebar.error(f"Could not load database info: {e}")


def check_for_updates():
    """
    Check if a newer version of the database is available.
    
    Returns:
        bool: True if update is available
    """
    # This could be implemented to check GitHub API for newer releases
    # For now, we'll keep it simple
    pass
