# ASX Stock Price Analysis & ML Predictions

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