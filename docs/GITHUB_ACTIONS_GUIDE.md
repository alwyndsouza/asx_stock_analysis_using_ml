# GitHub Actions Production Deployment Guide

## Overview

This guide explains how to deploy the ASX stock analysis pipeline to run automatically via GitHub Actions, with data stored in GitHub Releases for access by Streamlit dashboards.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  GitHub Actions (Scheduled Daily at 7PM AEST)          │
│  ┌────────────────────────────────────────────────┐    │
│  │ 1. Download existing DuckDB from Release       │    │
│  │ 2. Run extraction (full or incremental)        │    │
│  │ 3. Run dbt models (incremental)                │    │
│  │ 4. Upload updated DuckDB to Release            │    │
│  └────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  GitHub Release "data-latest"                           │
│  - asx_stocks.duckdb (persistent storage)              │
│  - dlt-state.tar.gz (pipeline state)                   │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Streamlit Apps (Local or Cloud)                       │
│  - Auto-downloads latest DuckDB on startup             │
│  - Uses db_utils.ensure_database_exists()              │
└─────────────────────────────────────────────────────────┘
```

## Setup Instructions

### 1. Enable GitHub Actions

1. Navigate to your repository on GitHub
2. Go to **Settings** → **Actions** → **General**
3. Under "Actions permissions", select **Allow all actions and reusable workflows**
4. Under "Workflow permissions", select **Read and write permissions**
5. Click **Save**

### 2. Verify Workflow File

The workflow file is already created at:
```
.github/workflows/daily-data-pipeline.yml
```

This workflow:
- Runs daily at 9:00 AM UTC (7:00 PM AEST approximately)
- Uses **uv** package manager for fast dependency installation
- Can be triggered manually via "Actions" tab
- First run does full load, subsequent runs are incremental
- Stores DuckDB and dlt state in GitHub Releases

**Key features:**
- ⚡ **Fast setup** with uv (10x faster than pip)
- 🔄 **Dependency caching** for even faster subsequent runs
- 📦 **Locked dependencies** with `uv sync --frozen`
- 🐍 **Python version management** via `uv python install`

### 3. Initial Setup (First Run)

#### Option A: Manual Trigger (Recommended)
1. Go to **Actions** tab in your GitHub repository
2. Click on "Daily ASX Data Pipeline" workflow
3. Click "Run workflow" button
4. Leave "Force full refresh" unchecked (it will auto-detect first run)
5. Click "Run workflow"

#### Option B: Wait for Scheduled Run
The workflow will run automatically at 7PM AEST.

### 4. Monitor the Pipeline

1. **View Workflow Runs:**
   - Go to **Actions** tab
   - Click on the latest run to see logs
   
2. **Check Release:**
   - Go to **Releases** section
   - Look for release tagged `data-latest`
   - You'll see the DuckDB file and metadata

3. **View Summary:**
   - Each workflow run creates a summary showing:
     - Number of records
     - Date range
     - Run type (full/incremental)

## Using the Data in Streamlit

### Method 1: Automatic Download (Recommended)

Your Streamlit apps can automatically download the latest database:

```python
# In your Streamlit app
from app.db_utils import get_database_connection, display_database_info

# Get connection (auto-downloads if needed)
con = get_database_connection()

# Display database info in sidebar
display_database_info()

# Query the data
df = con.execute("SELECT * FROM analytics.mart_ml_training_dataset").df()
```

The `db_utils` module will:
- Check if database exists locally
- Download from GitHub Releases if missing
- Cache the connection for performance

### Method 2: Manual Download

Download the database manually before starting Streamlit:

```bash
# Download latest database
python main.py download-db
# OR
python scripts/download_latest_db.py

# Start Streamlit app
python main.py dashboard
```

### Method 3: For Streamlit Cloud Deployment

Create a `packages.txt` file in your repo:
```
git
curl
```

Add a startup script `.streamlit/startup.sh`:
```bash
#!/bin/bash
python scripts/download_latest_db.py
```

Update `.streamlit/config.toml`:
```toml
[server]
runOnSave = false

[runner]
postScriptHook = false
```

## Workflow Behavior

### First Run (Full Load)
- Downloads 5 years of historical data
- Creates fresh DuckDB database
- Runs dbt with `--full-refresh`
- Creates GitHub Release with database
- **Duration:** ~10-15 minutes

### Subsequent Runs (Incremental)
- Downloads existing DuckDB from release
- Fetches only new data (last 30 days)
- Runs dbt incrementally
- Updates database in release
- **Duration:** ~2-3 minutes

### Force Full Refresh

If you need to rebuild everything:

1. Go to **Actions** tab
2. Click "Daily ASX Data Pipeline"
3. Click "Run workflow"
4. ✅ Check "Force full refresh"
5. Click "Run workflow"

## Schedule Configuration

### Current Schedule
- **Runs at:** 9:00 AM UTC
- **Equivalent to:** ~7:00 PM AEST (varies with daylight saving)

### Adjusting the Schedule

Edit `.github/workflows/daily-data-pipeline.yml`:

```yaml
on:
  schedule:
    # Cron format: minute hour day month weekday
    - cron: '0 9 * * *'  # 9:00 AM UTC
```

**Cron Examples:**
```yaml
'0 9 * * *'     # 9:00 AM UTC daily (7PM AEST)
'0 8 * * *'     # 8:00 AM UTC daily (7PM AEDT)
'0 9 * * 1-5'   # 9:00 AM UTC weekdays only
'0 */6 * * *'   # Every 6 hours
```

**Note:** GitHub Actions uses UTC time. AEST = UTC+10, AEDT = UTC+11.

## File Storage and Limits

### GitHub Releases
- **Storage:** Unlimited (for public repos)
- **File size limit:** 2 GB per file
- **Retention:** Permanent (until manually deleted)

### GitHub Artifacts (Backup)
- **Storage:** Limited by plan
- **Retention:** 30 days (can be configured)
- Used as backup in addition to releases

### Expected Database Size
- **Initial (5 years):** ~50-100 MB
- **After 1 year incremental:** ~60-110 MB
- **Growth rate:** ~1-2 MB per month

## Troubleshooting

### Workflow Fails on First Run

**Issue:** "No release found" or permission errors

**Solution:**
1. Check workflow permissions in Settings → Actions
2. Ensure "Read and write permissions" is enabled
3. Manually trigger the workflow

### Database Download Fails in Streamlit

**Issue:** Cannot download from GitHub

**Solution:**
1. Check if release exists: `https://github.com/YOUR_USERNAME/asx_stock_analysis_using_ml/releases/tag/data-latest`
2. Ensure repository is public (or add authentication for private repos)
3. Check internet connectivity

### Incremental Not Working

**Issue:** Always runs full load

**Solution:**
1. Check if dlt state is being preserved
2. Verify release contains `dlt-state.tar.gz`
3. Check workflow logs for state download errors

### Schedule Not Running

**Issue:** Workflow doesn't run at scheduled time

**Solutions:**
1. GitHub Actions can be delayed by up to 15 minutes
2. Check if Actions are enabled in repository settings
3. Verify cron syntax is correct
4. First scheduled run may take 24 hours to trigger

## Monitoring and Maintenance

### Weekly Checks
- ✅ Verify workflow ran successfully
- ✅ Check database size (shouldn't grow too quickly)
- ✅ Review data quality tests passed

### Monthly Tasks
- 📊 Review database size trends
- 🧹 Clean up old workflow runs (optional)
- 📈 Monitor incremental performance

### Quarterly Tasks
- 🔄 Consider full refresh to optimize database
- 📋 Review and update stock symbols list
- 🧪 Test disaster recovery (download and restore)

## Cost Considerations

### GitHub Actions (Free Tier)
- **Public repos:** Unlimited minutes ✅
- **Private repos:** 2,000 minutes/month
- Current pipeline: ~3 minutes/day = ~90 minutes/month

### Storage
- **Releases:** Free for public repos ✅
- **Artifacts:** Limited by plan (90-day retention)

### Data Transfer
- **GitHub Releases:** Free for public repos ✅
- No bandwidth limits for public repos

## Security Considerations

### For Public Repositories
- ✅ No API keys needed (uses public Yahoo Finance data)
- ✅ Database contains only public stock data
- ✅ No PII or sensitive information

### For Private Repositories
If you make this private:
1. Update `scripts/download_latest_db.py` to use authentication
2. Add GitHub token to Streamlit secrets
3. Consider using private artifact storage (S3, Azure Blob)

## Alternative Storage Options

If you need more control or have private data:

### Option 1: AWS S3
```yaml
- name: Upload to S3
  run: |
    aws s3 cp ${{ env.DUCKDB_FILE }} \
      s3://your-bucket/asx_stocks.duckdb
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

### Option 2: Google Cloud Storage
```yaml
- name: Upload to GCS
  uses: google-github-actions/upload-cloud-storage@v1
  with:
    path: ${{ env.DUCKDB_FILE }}
    destination: your-bucket/asx_stocks.duckdb
```

### Option 3: Azure Blob Storage
Similar configuration using Azure CLI or actions.

## Disaster Recovery

### Backup Strategy
1. **Primary:** GitHub Release (data-latest)
2. **Secondary:** Workflow artifacts (30-day retention)
3. **Tertiary:** Local backups (manual)

### Recovery Procedure
1. Download latest database:
   ```bash
   python scripts/download_latest_db.py
   ```
2. Verify data integrity:
   ```bash
   python -m ingestion.asx_extraction.extract info
   ```
3. If corrupted, trigger full refresh in Actions

## Next Steps

1. ✅ Push workflow file to GitHub
2. ✅ Enable GitHub Actions
3. ✅ Trigger first manual run
4. ✅ Verify release created
5. ✅ Test database download locally
6. ✅ Update Streamlit apps to use `db_utils`
7. ✅ Deploy Streamlit to cloud (optional)

## Support and Resources

- **Workflow Logs:** Actions tab in GitHub
- **Release Assets:** Releases section
- **Issues:** GitHub Issues tab
- **Documentation:**  
  - [GitHub Actions Docs](https://docs.github.com/en/actions)
  - [dlt Documentation](https://dlthub.com/docs)
  - [dbt Documentation](https://docs.getdbt.com/)

---

🎉 **Your pipeline is now production-ready with automated daily updates!**
