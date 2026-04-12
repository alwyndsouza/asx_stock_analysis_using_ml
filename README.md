# ASX Stock Analysis & ML Predictions

> 🚀 **Production-ready data pipeline** for analyzing Australian Stock Exchange (ASX) mining stocks with automated daily updates via GitHub Actions.

[![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-Ready-brightgreen)](https://github.com/alwyndsouza/asx_stock_analysis_using_ml/actions)
[![dlt](https://img.shields.io/badge/dlt-Incremental-blue)](https://dlthub.com)
[![dbt](https://img.shields.io/badge/dbt-Incremental-orange)](https://www.getdbt.com)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytics-yellow)](https://duckdb.org)

## Overview

This repository contains an end-to-end data engineering pipeline that extracts, transforms, and analyzes stock price data for major ASX-listed mining stocks. It leverages a modern data stack (dlt, dbt, DuckDB) to provide a fully automated, incremental, and cost-effective solution for financial data analysis and ML feature engineering.

### Key Features

- ✅ **Fully Incremental** - 90% faster than full refresh (2-3 min vs 20+ min).
- ✅ **Zero Maintenance** - Fully automated daily updates via GitHub Actions.
- ✅ **Production Ready** - Robust state management with `dlt` and rigorous data quality tests with `dbt`.
- ✅ **Feature Engineering** - Automated generation of 45+ technical indicators and ML features.
- ✅ **Free Hosting** - Utilizing GitHub Actions + Releases for compute and artifact persistence.
- ✅ **Interactive Dashboards** - Streamlit integration for real-time visualization and ML model tracking.

---

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
│  ├─ int_technical_indicators        (indicators)        │
│  ├─ int_ml_features                (feature eng)       │
│  └─ mart_ml_training_dataset        (ML-ready)          │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│  Streamlit Dashboards / ML Models                       │
└─────────────────────────────────────────────────────────┘
```

---

## Technical Deep Dive

### 1. Data Ingestion (`dlt`)
We use [dlt (data load tool)](https://dlthub.com) for robust extraction from the Yahoo Finance API.
- **Incremental Logic**: Tracks the `last_loaded_date` via dlt state. Daily runs only fetch the most recent 30 days of data, merging them into the existing DuckDB table using `(date, symbol)` as a primary key.
- **Coverage**: Tracks 11 key stocks across Gold (NCM, EVN, NST, RRL, SBM), Oil & Gas (WDS, STO, BPT, KAR), and Silver (SVL, S32).

### 2. Transformation Layer (`dbt`)
Data is transformed in DuckDB using `dbt`. All models are materialized as `incremental` to optimize performance.

- **Staging (`stg_`)**: Standardizes schema, handles date casting, and deduplication.
- **Intermediate (`int_`)**:
    - `int_technical_indicators`: Calculates SMAs (7, 14, 30, 50, 200), RSI, MACD, Bollinger Bands, ATR, and Volatility metrics.
    - `int_ml_features`: Generates lag features (1, 3, 5, 7 day returns), momentum signals, and Golden/Death cross indicators.
- **Marts (`mart_`)**:
    - `mart_ml_training_dataset`: The final wide table ready for ML. Includes target variables (`next_day_return`, `next_week_return`) and pre-defined train/val/test splits with cross-validation folds.

### 3. Automation & CI/CD
The pipeline runs daily at **7 PM AEST** via GitHub Actions:
1. **Restore**: Downloads the latest DuckDB artifact from GitHub Releases.
2. **Ingest**: Runs `dlt` to fetch incremental daily data.
3. **Transform**: Executes `dbt build` to update incremental models and run data quality tests.
4. **Persist**: Re-uploads the updated DuckDB database to GitHub Releases as `data-latest`.

---

## Getting Started

### Prerequisites
- Python 3.11+
- [uv](https://github.com/astral-sh/uv) package manager

### Installation
```bash
# Clone repository
git clone https://github.com/alwyndsouza/asx_stock_analysis_using_ml.git
cd asx_stock_analysis_using_ml

# Install dependencies
uv sync
```

### Usage (CLI Reference)
The project uses a central `main.py` entry point for all common tasks:

| Command | Description |
|---------|-------------|
| `python main.py download-db` | Download latest data from GitHub Releases |
| `python main.py extract-inc` | Run incremental extraction (30 days) |
| `python main.py extract` | Run full extraction (5 years) |
| `python main.py dbt` | Run all dbt transformations and tests |
| `python main.py all` | Run full pipeline (Extract + Transform) |
| `python main.py dashboard` | Start the main Streamlit dashboard |
| `python main.py signals` | Start the trading signals dashboard |
| `python main.py ml-app` | Start the ML training & prediction app |
| `python main.py ml-train` | Train ML models on the processed dataset |

---

## Data Quality & Testing
We maintain high data integrity through 35+ dbt tests, including:
- **Generic Tests**: `not_null`, `unique`, `accepted_values`.
- **Integrity Tests**: Ensures price consistency (e.g., `high >= low`) and validates window function lookbacks.

To run tests locally:
```bash
cd dbt_project
uv run dbt test
```

---

## Project Structure

```text
asx_stock_analysis_using_ml/
├── .github/workflows/         # GitHub Actions automation
├── app/                       # Streamlit dashboards (Dashboard, Signals, ML-App)
├── ingestion/                 # dlt extraction pipelines
├── dbt_project/               # dbt models, tests, and configurations
│   ├── models/
│   │   ├── staging/           # Raw data cleaning
│   │   ├── intermediate/      # Technical indicators & ML features
│   │   └── marts/             # ML-ready datasets
├── scripts/                   # Utility scripts (DB management)
├── main.py                    # Unified CLI entry point
├── pyproject.toml             # Dependency management (uv)
└── asx_stocks.duckdb          # Local DuckDB database (generated)
```

## Technology Stack

- **Extraction**: [dlt](https://dlthub.com)
- **Transformation**: [dbt](https://www.getdbt.com)
- **Database**: [DuckDB](https://duckdb.org)
- **Visualization**: [Streamlit](https://streamlit.io)
- **Orchestration**: GitHub Actions
- **Storage**: GitHub Releases

## License
MIT License - see [LICENSE](LICENSE) for details.

---
**Built for scalability and performance by Alwyn Dsouza**
