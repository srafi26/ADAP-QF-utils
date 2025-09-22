# Complete Contributor Deletion Sequence Guide

## 🚀 Quick Start (No Job URLs Required)

### Option 1: Simple Shell Script (Recommended)
```bash
# Test with 5 contributors (safe dry-run)
./run_deletion.sh --dry-run --sample-size 5

# Actually delete 10 contributors
./run_deletion.sh --execute --sample-size 10

# Delete without Redis clearing
./run_deletion.sh --execute --skip-redis
```

### Option 2: Direct Script Usage
```bash
# Fetch inactive contributors
python3 scripts/fetch_inactive_contributors.py --config ~/config_integration.ini --integration --execute

# Delete contributors (dry-run first!)
python3 scripts/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --dry-run

# Execute deletion
python3 scripts/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --execute
```

## 📋 What Each Script Does

### 1. `fetch_inactive_contributors.py` (Data Fetching)
- **Purpose**: Fetch inactive contributors from database
- **Features**:
  - PostgreSQL querying
  - CSV export
  - Configurable criteria
- **Usage**: `python3 scripts/fetch_inactive_contributors.py --config ~/config_integration.ini --integration --execute`

### 2. `delete_contributors_csv.py` (Core Deletion)
- **Purpose**: Main contributor deletion script
- **Features**:
  - Multi-system deletion/masking
  - Comprehensive logging
  - Dry-run mode
  - Error handling and recovery
- **Usage**: `python3 scripts/delete_contributors_csv.py --csv contributors.csv --config ~/config_integration.ini --integration --execute`

## 🔧 Individual Scripts (If You Need Them)

### Fetch Inactive Contributors
```bash
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --execute \
  --sample-size 10
```

### Delete Contributors from CSV
```bash
python3 scripts/delete_contributors_csv.py \
  --csv "backups/inactive_contributors_YYYYMMDD_HHMMSS.csv" \
  --config ~/config_integration.ini \
  --integration \
  --execute
```

### Test APIs (Optional - No Job URLs Required)
```bash
python3 scripts/test_fetch_commit_apis.py \
  --csv "backups/inactive_contributors_YYYYMMDD_HHMMSS.csv"
```

## 🎯 Complete Sequence (Step by Step)

### Step 1: Test the Process (Safe)
```bash
# Test with a small number of contributors
./run_deletion.sh --dry-run --sample-size 3
```

### Step 2: Run Actual Deletion
```bash
# Delete contributors (will ask for confirmation)
./run_deletion.sh --execute --sample-size 10
```

### Step 3: Verify Results
```bash
# Check if emails are still visible (should show "not found")
python3 check_specific_emails.py

# Check if DELETED_USER entries exist (should show some results)
python3 check_deleted_user.py
```

## 🌟 Real-World Example Sequence

### Real Project Example
```bash
# Step 1: Unit routing and testing with real project data
python scripts/automation/unified_unit_routing_and_testing_script.py \
  --mode combined \
  --project-id "83d4a405-cd08-4895-af71-d8e6b7f953b2" \
  --csv "backups/single_contributor_test.csv" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --single-unit

# Step 2: Delete contributors with real configuration
python scripts/api-testing/delete_contributors_csv.py \
  --csv backups/single_contributor_test.csv \
  --config ~/config_integration.ini \
  --integration \
  --execute \
  --skip-redis
```

### What This Real Example Does:
1. **Unit Routing**: Routes units to the specified job using real project ID `83d4a405-cd08-4895-af71-d8e6b7f953b2`
2. **API Testing**: Tests fetch/commit APIs with the real job URL
3. **Contributor Deletion**: Deletes contributors from all systems using integration environment
4. **Redis Skip**: Skips Redis session clearing (useful for testing environments)

## 🔍 Verification Commands

### Check Specific Emails
```bash
# Check if specific emails are still visible
python3 check_specific_emails.py
```

### Check DELETED_USER Entries
```bash
# Check if masking worked (should find DELETED_USER entries)
python3 check_deleted_user.py
```

### Check Project Indices
```bash
# Check project-specific indices for email visibility
python3 check_project_indices.py
```

## ⚙️ Configuration

### Required Files
- `~/config_integration.ini` - Configuration file with database and service URLs
- `scripts/` directory - Contains all the deletion scripts

### Optional Arguments
- `--sample-size N` - Number of contributors to process (default: 10)
- `--skip-redis` - Skip Redis session clearing
- `--execute` - Actually perform deletion (without this, it's dry-run)

## 🚨 Safety Features

### Dry Run Mode
- **Default behavior**: All scripts run in dry-run mode unless `--execute` is specified
- **Safe testing**: You can test with `--dry-run` to see what would happen
- **No data loss**: Dry-run mode doesn't actually delete anything

### Confirmation Prompts
- **Shell script**: Asks for confirmation before actual deletion
- **Sample size**: Start with small numbers (3-5) for testing
- **Backup creation**: Scripts automatically create CSV backups

## 📊 What Gets Deleted/Masked

### PostgreSQL
- ✅ Contributor records deleted
- ✅ Job mappings deleted (prevents platform access)
- ✅ PII data masked with "DELETED_USER"

### Elasticsearch
- ✅ Email addresses masked with "DELETED_USER" in project indices
- ✅ Worker IDs masked with "DELETED_USER"
- ✅ All nested email fields masked

### ClickHouse
- ✅ Email addresses masked with "DELETED_USER"
- ✅ Handles Kafka table engine gracefully

### Redis (Optional)
- ✅ Session data cleared (can be skipped with `--skip-redis`)

### S3
- ✅ Files containing contributor IDs deleted

## 🔧 Troubleshooting

### Common Issues
1. **"No contributors found"**: Try increasing `--sample-size`
2. **"Config file not found"**: Ensure `~/config_integration.ini` exists
3. **"Permission denied"**: Run `chmod +x run_deletion.sh`

### Logs
- All scripts create detailed logs in the `logs/` directory
- Check logs for detailed error information

## 📝 Example Complete Workflow

```bash
# 1. Test with 3 contributors (safe)
./run_deletion.sh --dry-run --sample-size 3

# 2. If test looks good, delete 10 contributors
./run_deletion.sh --execute --sample-size 10

# 3. Verify deletion worked
python3 check_specific_emails.py

# 4. Check if masking worked
python3 check_deleted_user.py
```

## 🎉 Success Indicators

- ✅ Script completes without errors
- ✅ Original emails not found in Elasticsearch
- ✅ DELETED_USER entries found in Elasticsearch
- ✅ Contributors cannot access the platform
- ✅ Logs show successful deletion/masking

---

**Note**: This system is designed to work without requiring job URLs. It automatically fetches inactive contributors and deletes them from all relevant data sources.
