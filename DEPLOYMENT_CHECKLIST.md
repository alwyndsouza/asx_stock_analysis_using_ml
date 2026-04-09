# Production Deployment Checklist

Use this checklist to deploy your pipeline to production.

## Pre-Deployment ☑️

- [ ] All code pushed to GitHub
- [ ] Review `.gitignore` (DuckDB and dlt state excluded)
- [ ] Test extraction locally: `python main.py extract-inc`
- [ ] Test dbt locally: `cd dbt_project && uv run dbt build`
- [ ] Test database download: `python main.py download-db`
- [ ] Verify all tests pass: `cd dbt_project && uv run dbt test`

## GitHub Actions Setup ☑️

- [ ] Navigate to repository on GitHub
- [ ] Go to **Settings** → **Actions** → **General**
- [ ] Enable "Allow all actions and reusable workflows"
- [ ] Select "Read and write permissions" under Workflow permissions
- [ ] Click **Save**
- [ ] Verify workflow file exists: `.github/workflows/daily-data-pipeline.yml`

## First Run (Initial Load) ☑️

- [ ] Go to **Actions** tab in GitHub
- [ ] Click "Daily ASX Data Pipeline" workflow
- [ ] Click "Run workflow" button
- [ ] Leave "Force full refresh" unchecked
- [ ] Click "Run workflow" to start
- [ ] Wait 10-15 minutes for completion
- [ ] Check workflow completed successfully (green checkmark)
- [ ] View workflow summary for stats

## Verify Data Storage ☑️

- [ ] Go to **Releases** section in GitHub
- [ ] Verify `data-latest` release exists
- [ ] Confirm `asx_stocks.duckdb` file is attached (~50-100 MB)
- [ ] Confirm `dlt-state.tar.gz` file is attached (~1 MB)
- [ ] Check release notes show data statistics
- [ ] Note the public download URL

## Test Local Access ☑️

```bash
# Download database
- [ ] python main.py download-db

# Verify download
- [ ] ls -lh asx_stocks.duckdb

# View stats
- [ ] python -m ingestion.asx_extraction.extract info

# Test Streamlit
- [ ] python main.py dashboard
- [ ] Verify data loads correctly
- [ ] Check database info in sidebar
```

## Verify Incremental Updates ☑️

Option 1: Wait for next scheduled run (7PM AEST)
- [ ] Check Actions tab next day
- [ ] Verify workflow ran automatically
- [ ] Check it ran incremental (not full)
- [ ] Verify completion time ~2-3 minutes

Option 2: Manual trigger
- [ ] Go to Actions tab
- [ ] Run workflow manually
- [ ] Leave "Force full refresh" unchecked
- [ ] Verify incremental run (~2-3 min)
- [ ] Check release updated with new data

## Streamlit Cloud Deployment (Optional) ☑️

- [ ] Go to share.streamlit.io
- [ ] Sign in with GitHub
- [ ] Click "New app"
- [ ] Select your repository
- [ ] Choose branch: `main`
- [ ] Set main file: `app/dashboard.py`
- [ ] Advanced settings:
  - [ ] Python version: 3.11
  - [ ] No secrets needed
- [ ] Click "Deploy"
- [ ] Wait for deployment (~5 minutes)
- [ ] Verify app loads correctly
- [ ] Check database downloaded automatically
- [ ] Test all pages work

## Monitoring Setup ☑️

- [ ] Bookmark Actions page for monitoring
- [ ] Bookmark Releases page  
- [ ] Set up GitHub notifications (optional):
  - [ ] Go to repository settings
  - [ ] Notifications → Actions
  - [ ] Enable failure notifications
- [ ] Add repository to watch list
- [ ] Document expected database size growth (~1-2 MB/month)

## Weekly Maintenance ☑️

- [ ] Check workflow ran successfully
- [ ] Verify data freshness (date range in release notes)
- [ ] Monitor database size
- [ ] Review dbt test results
- [ ] Check for any errors in logs

## Monthly Tasks ☑️

- [ ] Review pipeline performance trends
- [ ] Check database size vs expectations
- [ ] Verify all stock symbols still valid
- [ ] Review and clean up old workflow runs (optional)
- [ ] Update documentation if process changes

## Quarterly Tasks ☑️

- [ ] Consider full refresh for database optimization
  - [ ] Go to Actions → Run workflow
  - [ ] Check "Force full refresh"
  - [ ] Run and verify
- [ ] Review and update stock symbols list
- [ ] Test disaster recovery:
  - [ ] Delete local database
  - [ ] Re-download from GitHub
  - [ ] Verify integrity
- [ ] Update dependencies if needed

## Troubleshooting Checklist ☑️

### If workflow fails:
- [ ] Check Actions logs for error details
- [ ] Verify permissions still enabled
- [ ] Check if Yahoo Finance API accessible
- [ ] Try manual trigger with full refresh
- [ ] Review dbt error messages

### If database download fails:
- [ ] Verify release exists and has assets
- [ ] Check repository is public (or auth configured for private)
- [ ] Test download URL in browser
- [ ] Check network connectivity
- [ ] Review error messages in script

### If incremental not working:
- [ ] Verify dlt state is preserved in release
- [ ] Check workflow logs for state download
- [ ] Look for "first_run" detection in logs
- [ ] Try deleting release and running fresh

### If Streamlit app crashes:
- [ ] Check database file exists and is valid
- [ ] Review Streamlit Cloud logs
- [ ] Verify startup script ran successfully
- [ ] Test database connection locally
- [ ] Check Python dependencies installed

## Success Criteria ✅

Your deployment is successful when:

- ✅ Workflow runs automatically daily at 7PM AEST
- ✅ Each run completes in ~2-3 minutes (incremental)
- ✅ Release is updated with new data daily
- ✅ Database size grows predictably (~1-2 MB/month)
- ✅ All dbt tests pass
- ✅ Streamlit apps load latest data automatically
- ✅ No manual intervention needed for weeks

## Documentation Review ☑️

Have you read:
- [ ] [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md) - Overview
- [ ] [docs/GITHUB_ACTIONS_GUIDE.md](docs/GITHUB_ACTIONS_GUIDE.md) - Complete guide
- [ ] [INCREMENTAL_UPDATES_SUMMARY.md](INCREMENTAL_UPDATES_SUMMARY.md) - Technical details
- [ ] [.streamlit/README.md](.streamlit/README.md) - Streamlit config

## Final Steps ☑️

- [ ] Share deployment with team
- [ ] Document any custom configurations
- [ ] Set calendar reminder for monthly check
- [ ] Celebrate! 🎉 Your pipeline is production-ready!

---

## Quick Reference Commands

```bash
# Download latest data
python main.py download-db

# View database stats  
python -m ingestion.asx_extraction.extract info

# Run full pipeline locally
python main.py all

# Start Streamlit apps
python main.py dashboard
python main.py signals  
python main.py ml-app

# Force full refresh in GitHub Actions
# Go to Actions → Run workflow → Check "Force full refresh"
```

## Support Resources

- **GitHub Actions Logs:** Actions tab → Latest run
- **Release Assets:** Releases → data-latest
- **Workflow File:** `.github/workflows/daily-data-pipeline.yml`
- **Download Script:** `scripts/download_latest_db.py`
- **Streamlit Utils:** `app/db_utils.py`

---

**Date Completed:** _________________

**Deployed By:** _________________

**Notes:** _________________
