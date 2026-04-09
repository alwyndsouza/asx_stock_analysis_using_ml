# ASX Stock Analysis & ML Predictions

> 🚀 **Production-ready data pipeline** for analyzing Australian Stock Exchange (ASX) mining stocks with automated daily updates via GitHub Actions.

[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-Ready-brightgreen)](https://github.com/alwyndsouza/asx_stock_analysis_using_ml/actions)
[![dlt](https://img.shields.io/badge/dlt-Incremental-blue)](https://dlthub.com)
[![dbt](https://img.shields.io/badge/dbt-Incremental-orange)](https://www.getdbt.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow)](https://duckdb.org)

## Overview

End-to-end data engineering pipeline that:
- 📊 **Extracts** stock price data from Yahoo Finance (5 years history)
- 🔄 **Transforms** with dbt (technical indicators & ML features)
- 🤖 **Automates** via GitHub Actions (daily at 7PM AEST)
- 💾 **Stores** in DuckDB (GitHub Releases for persistence)
- 📈 **Serves** Streamlit dashboards with auto-updating data

### Key Features

✅ **Fully Incremental** - 90% faster than full refresh (2-3 min vs 15-25 min)  
✅ **Zero Maintenance** - Automated daily updates  
✅ **Free Hosting** - GitHub Actions + Releases (public repos)  
✅ **Production Ready** - dlt state management + dbt tests  
✅ **Streamlit Integration** - Auto-downloads latest data  

## Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager
- Git

### Installation

```bash
# Clone repository
git clone https://github.com/alwyndsouza/asx_stock_analysis_using_ml.git
cd asx_stock_analysis_using_ml

# Install dependencies
uv sync
```

### Local Development

```bash
# Option 1: Download latest data from GitHub (if pipeline is deployed)
python main.py download-db

# Option 2: Run extraction yourself
python main.py extract          # Full load (5 years)
python main.py extract-inc      # Incremental (30 days)

# Run dbt transformations
python main.py dbt

# Start Streamlit dashboard
python main.py dashboard
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Yahoo Finance API                                      │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  dlt Pipeline (Incremental Extraction)                  │
│  - Fetches OHLCV data for 13 ASX stocks                │
│  - Automatic state management                           │
│  - Merge on [date, symbol]                             │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  DuckDB (raw_asx_data.asx_stock_prices)                │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  dbt Models (Incremental)                               │
│  ├─ stg_asx_stock_prices           (staging)           │
│  ├─ int_technical_indicators        (200-day lookback) │
│  ├─ int_ml_features                (7-day lookback)    │
│  └─ mart_ml_training_dataset        (21-day lookback)  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  Streamlit Dashboards / ML Models                       │
└─────────────────────────────────────────────────────────┘
```

## Data Flow

### Stock Coverage

- **Gold Mining** (5 stocks): NCM.AX, EVN.AX, NST.AX, RRL.AX, SBM.AX
- **Oil & Gas** (4 stocks): WDS.AX, STO.AX, BPT.AX, KAR.AX
- **Silver Mining** (2 stocks): SVL.AX, S32.AX

### dbt Models

#### 1. Staging (`models/staging/`)
- **stg_asx_stock_prices**: Cleaned OHLCV data with date casting and deduplication

#### 2. Intermediate (`models/intermediate/`)
- **int_technical_indicators**: 
  - Moving Averages: SMA 7, 14, 30, 50, 200
  - Momentum: RSI (14-period)
  - Trend: MACD (12/26/9), Bollinger Bands
  - Volatility: ATR (14-period), 20-day volatility
  - Volume: OBV, Volume SMA
  
- **int_ml_features**:
  - Lag features (1, 3, 5, 7 day returns)
  - Momentum indicators (7, 14, 30 day)
  - Golden/Death cross signals
  - Support/Resistance levels
  - Sector correlation metrics

#### 3. Marts (`models/marts/`)
- **mart_ml_training_dataset**:
  - All features from intermediate models
  - Target variables: next_day_return, next_week_return, next_month_return
  - Train/validation/test splits
  - Time-series cross-validation folds

## Production Deployment

### GitHub Actions Pipeline

The pipeline runs automatically **daily at 7PM AEST** via GitHub Actions:

1. **Downloads** existing DuckDB from GitHub Release
2. **Extracts** new data (dlt incremental)
3. **Transforms** data (dbt incremental)
4. **Tests** data quality (dbt test)
5. **Uploads** updated DuckDB to Release

### Performance

| Metric | Full Refresh | Incremental |
|--------|-------------|-------------|
| Extraction | ~10-15 min | ~30 sec |
| dbt Build | ~5-10 min | ~1-2 min |
| **Total** | **~15-25 min** | **~2-3 min** |
| Data Fetched | 5 years | 30 days |
| Processing | All records | New only |

**Daily cost:** $0 (free for public repos)

### Setup Instructions

See the complete deployment guide: **[docs/GITHUB_ACTIONS_GUIDE.md](docs/GITHUB_ACTIONS_GUIDE.md)**

**Quick Setup:**

1. **Enable GitHub Actions**
   - Go to Settings → Actions → General
   - Select "Read and write permissions"
   
2. **Trigger First Run**
   - Go to Actions tab
   - Run "Daily ASX Data Pipeline" workflow
   - Wait ~10-15 minutes
   
3. **Verify**
   - Check Releases section for `data-latest`
   - Download database locally

4. **Deploy Streamlit** (optional)
   - App auto-downloads database from GitHub
   - Works on Streamlit Cloud

## Streamlit Integration

Your Streamlit apps automatically download the latest database:

```python
from app.db_utils import get_database_connection, display_database_info

# Auto-downloads if missing
con = get_database_connection()

# Query transformed data
df = con.execute("""
    SELECT * FROM analytics.mart_ml_training_dataset
    WHERE symbol = 'NCM.AX'
    ORDER BY price_date DESC
    LIMIT 100
""").df()

# Show database info in sidebar
display_database_info()
```

## Commands Reference

```bash
# Data Extraction
python main.py extract          # Full load (5 years)
python main.py extract-inc      # Incremental load (30 days)

# dbt Transformations
python main.py dbt              # Run dbt (incremental)
cd dbt_project && uv run dbt build --full-refresh  # Force full

# Database Management
python main.py download-db      # Download from GitHub Release
python -m ingestion.asx_extraction.extract info  # View stats

# Streamlit Apps
python main.py dashboard        # Main dashboard
python main.py signals          # Trading signals
python main.py ml-app           # ML training interface

# Full Pipeline
python main.py all              # Extract + Transform
```

## Project Structure

```
asx_stock_analysis_using_ml/
├── .github/workflows/
│   └── daily-data-pipeline.yml       # GitHub Actions workflow
│
├── app/
│   ├── dashboard.py                  # Main Streamlit dashboard
│   ├── signals.py                    # Trading signals app
│   ├── ml_app.py                     # ML training app
│   └── db_utils.py                   # Database utilities
│
├── ingestion/
│   └── asx_extraction/
│       └── extract.py                # dlt extraction pipeline
│
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # Cleaned source data
│   │   ├── intermediate/             # Technical indicators & features
│   │   └── marts/                    # ML-ready datasets
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── scripts/
│   └── download_latest_db.py         # Download database from GitHub
│
├── docs/
│   └── GITHUB_ACTIONS_GUIDE.md       # Deployment guide
│
├── main.py                           # CLI launcher
├── pyproject.toml                    # Dependencies (uv)
└── README.md                         # This file
```

## Incremental Loading

### How It Works

**dlt (Extraction):**
- Tracks last loaded date via state files
- Fetches only new data on each run
- Merges using `[date, symbol]` as primary key
- State persisted in `.dlt/` directory

**dbt (Transformation):**
- All models use `materialized='incremental'`
- Lookback windows for window functions:
  - `stg_asx_stock_prices`: No lookback needed
  - `int_technical_indicators`: 200 days (for SMA_200)
  - `int_ml_features`: 7 days (for lag features)
  - `mart_ml_training_dataset`: 21 days (for targets)

### Benefits

- **90% faster** for daily updates
- **Reduced API calls** to Yahoo Finance
- **Lower costs** (less compute time)
- **Automatic state management** (no manual tracking)

## Development

### Running Tests

```bash
# dbt tests (data quality)
cd dbt_project
uv run dbt test

# Run specific test
uv run dbt test --select stg_asx_stock_prices
```

### Adding New Stocks

Edit `ingestion/asx_extraction/extract.py`:

```python
ASX_STOCKS = {
    "gold": ["NCM.AX", "EVN.AX", "NEW.AX"],  # Add new stock
    "oil": ["WDS.AX", "STO.AX"],
    "silver": ["SVL.AX", "S32.AX"],
}
```

Then run full refresh:
```bash
python main.py extract
cd dbt_project && uv run dbt build --full-refresh
```

### Customizing Schedule

Edit `.github/workflows/daily-data-pipeline.yml`:

```yaml
on:
  schedule:
    - cron: '0 9 * * *'  # 9:00 AM UTC = 7PM AEST
```

## Troubleshooting

### Pipeline Issues

**Workflow fails:**
- Check GitHub Actions logs
- Verify permissions enabled
- Try manual trigger with full refresh

**Database download fails:**
- Ensure release exists: `https://github.com/YOUR_USERNAME/asx_stock_analysis_using_ml/releases/tag/data-latest`
- Check repository is public
- Verify internet connectivity

**Incremental not working:**
- Check dlt state preserved in release
- Review workflow logs
- Try deleting `.dlt/` and running full refresh

### Data Quality

```sql
-- Check for gaps in data
SELECT 
    symbol,
    MIN(price_date) as min_date,
    MAX(price_date) as max_date,
    COUNT(*) as row_count,
    COUNT(DISTINCT price_date) as unique_dates
FROM analytics.stg_asx_stock_prices
GROUP BY symbol;

-- Verify latest data
SELECT MAX(price_date) FROM analytics.mart_ml_training_dataset;
```

## Documentation

- **[GitHub Actions Guide](docs/GITHUB_ACTIONS_GUIDE.md)** - Complete deployment guide
- **[Deployment Checklist](DEPLOYMENT_CHECKLIST.md)** - Step-by-step deployment
- **[Streamlit Config](.streamlit/README.md)** - Streamlit Cloud setup

## Technology Stack

- **Data Extraction**: [dlt](https://dlthub.com) (incremental loading framework)
- **Transformation**: [dbt](https://www.getdbt.com) (data transformation)
- **Database**: [DuckDB](https://duckdb.org) (embedded analytics)
- **Orchestration**: GitHub Actions (CI/CD)
- **Storage**: GitHub Releases (artifact storage)
- **Visualization**: Streamlit (dashboards)
- **Package Management**: [uv](https://github.com/astral-sh/uv) (fast Python package manager)

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

- **Issues**: GitHub Issues tab
- **Actions Logs**: Actions tab in repository
- **Releases**: Check for latest data

---

**Built with ❤️ using dlt + dbt + DuckDB + GitHub Actions**

A data engineering pipeline for analyzing ASX-listed mining stocks (gold, oil, silver) and building ML models for price prediction using technical indicators.

## Project Structure

```
asx_stock_analysis_using_ml/
├── pyproject.toml              # Project configuration (UV)
├── README.md                   # This file
│
├── ingestion/                  # Data extraction pipeline (dlt)
│   └── asx_extraction/
│       ├── __init__.py
│       └── extract.py          # dlt extraction from Yahoo Finance
│
├── dbt_project/                # dbt models for transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── sources.yml
│       ├── staging/
│       ├── intermediate/
│       └── marts/
│
└── asx_stocks.duckdb           # DuckDB database
```

## Features

### 1. Data Extraction (dlt)
- Fetches 5 years of historical OHLCV data from Yahoo Finance
- Supports 13 ASX mining stocks across 3 sectors:
  - **Gold**: NCM.AX, EVN.AX, NST.AX, RRL.AX, SBM.AX
  - **Oil**: WDS.AX, STO.AX, BPT.AX, KAR.AX
  - **Silver**: SVL.AX, S32.AX
- Loads data into DuckDB using dlt pipeline

### 2. dbt Transformations

#### Staging Layer (`models/staging/`)
- `stg_asx_stock_prices` - Clean and standardize raw stock data

#### Intermediate Layer (`models/intermediate/`)
- `int_technical_indicators` - Technical indicators:
  - SMAs (7, 14, 30, 50, 200 day)
  - RSI (14-period)
  - MACD (12/26/9)
  - Bollinger Bands
  - ATR (14-period)
  - Volume indicators (OBV, volume SMA)
  - Volatility (20-day std dev)

- `int_ml_features` - ML feature engineering:
  - Lag features (1, 3, 5, 7 day returns)
  - Momentum (rate of change)
  - Golden cross / death cross signals
  - Support/resistance levels
  - Price position indicators

#### Marts Layer (`models/marts/`)
- `mart_ml_training_dataset` - Final ML-ready dataset with:
  - All technical indicators as features
  - Target variables (next_day_return, next_week_return)
  - Train/validation/test splits
  - Cross-validation fold numbers

## Installation

### Prerequisites
- Python 3.11+
- [UV](https://github.com/astral-sh/uv) package manager

### Setup

```bash
# Install dependencies with UV
uv sync

# Install dev dependencies (linting, formatting)
uv sync --group dev
```

## Usage

### 1. Extract Data

```bash
# Run dlt extraction pipeline
python -m ingestion.asx_extraction.extract
```

Or use the CLI entry point:

```bash
asx-extract
```

### 2. Run dbt Models

```bash
cd dbt_project

# Install dependencies
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

### 3. ML Training

Use the `mart_ml_training_dataset` table to train ML models:

```python
import duckdb
from sklearn.ensemble import RandomForestRegressor

# Load data
con = duckdb.connect('asx_stocks.duckdb')
df = con.execute("SELECT * FROM analytics.mart_ml_training_dataset").df()

# Train model
model = RandomForestRegressor(n_estimators=100)
model.fit(X_train, y_train)
```

## Configuration

### pyproject.toml

```toml
[project]
name = "asx-stock-analysis"
version = "0.1.0"

[dependency-groups]
dev = [
    "ruff>=0.4.0",
    "black>=24.0.0",
    "isort>=5.13.0",
]
```

### dbt profiles.yml

```yaml
asx_stock_analysis:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "../asx_stocks.duckdb"
      schema: analytics
```

## Linting & Formatting

The project uses:
- **Ruff** - Fast linter
- **Black** - Code formatter
- **isort** - Import sorter

```bash
# Run linting
ruff check .

# Run formatting
black .
isort .

# Or run all at once (if using pre-commit)
pre-commit install
pre-commit run --all-files
```

## Database Schema

### Source: `raw_asx_data.asx_stock_prices`
| Column | Type | Description |
|--------|------|-------------|
| date | TIMESTAMP | Trading date |
| symbol | VARCHAR | ASX ticker (e.g., EVN.AX) |
| sector | VARCHAR | gold/oil/silver |
| open_price | DOUBLE | Opening price |
| high_price | DOUBLE | High price |
| low_price | DOUBLE | Low price |
| close_price | DOUBLE | Closing price |
| volume | BIGINT | Trading volume |
| extraction_timestamp | VARCHAR | Extraction time |

### Mart: `analytics.mart_ml_training_dataset`
Contains 45+ features including:
- All technical indicators
- Lag features
- Momentum indicators
- Cross signals
- Support/resistance levels
- Target variables: `next_day_return`, `next_week_return`
- Split markers: `split` (train/validation/test), `fold_number`

## Tests

38 data quality tests are configured across all models:
- Not null constraints on key fields
- Accepted values for categorical columns
- Expression tests for data integrity

Run tests with:
```bash
cd dbt_project
dbt test
```

## License

MIT