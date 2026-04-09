#!/usr/bin/env python3
"""
Download the latest DuckDB database from GitHub Releases.

This script is used by Streamlit apps and local development to get
the latest data processed by the GitHub Actions pipeline.

Usage:
    python scripts/download_latest_db.py
    python scripts/download_latest_db.py --force  # Force redownload
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from urllib.request import urlretrieve
import json
from datetime import datetime

# Configuration
GITHUB_REPO = os.getenv("GITHUB_REPOSITORY", "alwyndsouza/asx_stock_analysis_using_ml")
RELEASE_TAG = "data-latest"
DUCKDB_FILENAME = "asx_stocks.duckdb"
DLT_STATE_FILENAME = "dlt-state.tar.gz"

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / DUCKDB_FILENAME
DLT_STATE_PATH = PROJECT_ROOT / DLT_STATE_FILENAME

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_release_info():
    """Get release information from GitHub API."""
    import urllib.request
    
    api_url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{RELEASE_TAG}"
    
    try:
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())
            return data
    except Exception as e:
        logger.error(f"Failed to fetch release info: {e}")
        return None


def download_file(url: str, destination: Path, description: str = "file"):
    """Download a file with progress indication."""
    logger.info(f"Downloading {description}...")
    logger.info(f"URL: {url}")
    logger.info(f"Destination: {destination}")
    
    def report_progress(block_num, block_size, total_size):
        """Report download progress."""
        downloaded = block_num * block_size
        if total_size > 0:
            percent = min(100, (downloaded / total_size) * 100)
            mb_downloaded = downloaded / (1024 * 1024)
            mb_total = total_size / (1024 * 1024)
            
            if block_num % 100 == 0:  # Update every 100 blocks
                logger.info(f"Progress: {percent:.1f}% ({mb_downloaded:.1f}/{mb_total:.1f} MB)")
    
    try:
        urlretrieve(url, destination, reporthook=report_progress)
        logger.info(f"✅ Downloaded {description} successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to download {description}: {e}")
        return False


def download_latest_database(force: bool = False):
    """Download the latest database from GitHub releases."""
    
    # Check if database already exists
    if DB_PATH.exists() and not force:
        logger.info(f"Database already exists at {DB_PATH}")
        
        # Check file age
        file_age_hours = (datetime.now().timestamp() - DB_PATH.stat().st_mtime) / 3600
        logger.info(f"Database age: {file_age_hours:.1f} hours")
        
        if file_age_hours < 24:
            logger.info("Database is recent (< 24 hours). Use --force to redownload.")
            return True
        else:
            logger.info("Database is older than 24 hours. Downloading latest...")
    
    # Get release information
    logger.info(f"Fetching release information for '{RELEASE_TAG}'...")
    release_info = get_release_info()
    
    if not release_info:
        logger.error("Could not fetch release information")
        return False
    
    # Find DuckDB asset
    duckdb_asset = None
    dlt_state_asset = None
    
    for asset in release_info.get("assets", []):
        if asset["name"] == DUCKDB_FILENAME:
            duckdb_asset = asset
        elif asset["name"] == DLT_STATE_FILENAME:
            dlt_state_asset = asset
    
    if not duckdb_asset:
        logger.error(f"DuckDB file '{DUCKDB_FILENAME}' not found in release")
        return False
    
    # Display release information
    logger.info("=" * 60)
    logger.info("Release Information:")
    logger.info(f"  Tag: {release_info.get('tag_name')}")
    logger.info(f"  Published: {release_info.get('published_at')}")
    logger.info(f"  DuckDB Size: {duckdb_asset['size'] / (1024*1024):.1f} MB")
    logger.info("=" * 60)
    
    # Backup existing database if it exists
    if DB_PATH.exists():
        backup_path = DB_PATH.with_suffix(".duckdb.backup")
        logger.info(f"Backing up existing database to {backup_path}")
        DB_PATH.rename(backup_path)
    
    # Download DuckDB
    success = download_file(
        duckdb_asset["browser_download_url"],
        DB_PATH,
        "DuckDB database"
    )
    
    if not success:
        # Restore backup if download failed
        backup_path = DB_PATH.with_suffix(".duckdb.backup")
        if backup_path.exists():
            backup_path.rename(DB_PATH)
            logger.warning("Restored backup database due to download failure")
        return False
    
    # Download dlt state if exists (optional)
    if dlt_state_asset:
        download_file(
            dlt_state_asset["browser_download_url"],
            DLT_STATE_PATH,
            "dlt state"
        )
        
        # Extract dlt state
        if DLT_STATE_PATH.exists():
            import tarfile
            logger.info("Extracting dlt state...")
            with tarfile.open(DLT_STATE_PATH, "r:gz") as tar:
                tar.extractall(PROJECT_ROOT)
            logger.info("✅ Extracted dlt state")
    
    # Verify database
    logger.info("Verifying database...")
    try:
        import duckdb
        con = duckdb.connect(str(DB_PATH), read_only=True)
        
        # Get basic stats
        raw_count = con.execute("SELECT COUNT(*) FROM raw_asx_data.asx_stock_prices").fetchone()[0]
        date_range = con.execute("SELECT MIN(date), MAX(date) FROM raw_asx_data.asx_stock_prices").fetchone()
        
        logger.info("=" * 60)
        logger.info("Database Verification:")
        logger.info(f"  ✅ Raw records: {raw_count:,}")
        logger.info(f"  ✅ Date range: {date_range[0]} to {date_range[1]}")
        logger.info("=" * 60)
        
        con.close()
        
        # Remove backup
        backup_path = DB_PATH.with_suffix(".duckdb.backup")
        if backup_path.exists():
            backup_path.unlink()
            logger.info("Removed backup database")
        
        return True
        
    except Exception as e:
        logger.error(f"Database verification failed: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Download the latest ASX stock database from GitHub Releases"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force redownload even if database exists"
    )
    parser.add_argument(
        "--repo",
        default=GITHUB_REPO,
        help=f"GitHub repository (default: {GITHUB_REPO})"
    )
    
    args = parser.parse_args()
    
    # Override repo if specified
    global GITHUB_REPO
    GITHUB_REPO = args.repo
    
    logger.info("🚀 Starting database download...")
    logger.info(f"Repository: {GITHUB_REPO}")
    logger.info(f"Release tag: {RELEASE_TAG}")
    
    success = download_latest_database(force=args.force)
    
    if success:
        logger.info("✅ Database download completed successfully!")
        return 0
    else:
        logger.error("❌ Database download failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
