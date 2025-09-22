# 🚀 Quick Reference - Contributor Deletion System

## 🔧 Setup Commands

### 1. Environment Setup
```bash
# Set ClickHouse credentials
export CLICKHOUSE_USERNAME='kepler'
export CLICKHOUSE_PASSWORD='cLE8L3OEdr63'

# Setup ClickHouse port-forwarding
kubectl port-forward clickhouse-0 8123:8123 9000:9000

# Test connectivity
curl -s "http://localhost:8123/ping"
```

### 2. Configuration Files
- **Development**: `~/config.ini`
- **Integration**: `~/config_integration.ini`

## 📋 Common Commands

### 1. Fetch Inactive Contributors
```bash
# Dry run (safe)
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --dry-run

# Execute (create CSV)
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --execute
```

### 2. Delete Contributors (Dry Run First!)
```bash
# ALWAYS run dry-run first
python3 scripts/delete_contributors_csv.py \
  --csv "backups/inactive_contributors_10.csv" \
  --config ~/config_integration.ini \
  --integration \
  --dry-run

# Execute deletion
python3 scripts/delete_contributors_csv.py \
  --csv "backups/inactive_contributors_10.csv" \
  --config ~/config_integration.ini \
  --integration \
  --execute
```

### 3. Test APIs
```bash
# Test before deletion
python3 scripts/test_fetch_commit_apis.py \
  --csv "backups/inactive_contributors_10.csv" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --config ~/config_integration.ini \
  --integration \
  --sample-size 3

# Test after deletion (should fail)
python3 scripts/test_fetch_commit_apis.py \
  --csv "backups/inactive_contributors_10.csv" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --config ~/config_integration.ini \
  --integration \
  --sample-size 3 \
  --fetch-only
```

### 4. Complete Workflow
```bash
python3 comprehensive_testing_workflow.py \
  --project-id "fe92bf1f-46c3-4ebb-a09e-0557237f41e6" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --csv "backups/inactive_contributors_10.csv" \
  --config ~/config_integration.ini
```

### 5. Unit Routing and Testing (Real Example)
```bash
# Combined unit routing and testing
python scripts/unified_unit_routing_and_testing_script.py \
  --mode combined \
  --project-id "83d4a405-cd08-4895-af71-d8e6b7f953b2" \
  --csv "backups/single_contributor_test.csv" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --single-unit

# Delete contributors (real example)
python scripts/delete_contributors_csv.py \
  --csv backups/single_contributor_test.csv \
  --config ~/config_integration.ini \
  --integration \
  --execute \
  --skip-redis
```

## 🔍 Verification Commands

### 1. Check Elasticsearch Masking
```bash
curl -s -X GET "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/project-your-project-id/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "term": {
        "latest.workerEmail": "DELETED_USER"
      }
    }
  }'
```

### 2. Check ClickHouse Masking
```bash
curl -s -X POST "http://localhost:8123/" \
  -H "Content-Type: text/plain" \
  -d "SELECT COUNT(*) FROM kepler.unit_metrics WHERE email = 'DELETED_USER'"
```

### 3. Test Connectivity
```bash
# ClickHouse
curl -s "http://localhost:8123/ping"

# Elasticsearch
curl -s "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/_cluster/health"

# PostgreSQL (test connection)
python3 -c "
import psycopg2
import configparser
config = configparser.ConfigParser()
config.read(os.path.expanduser('~/config_integration.ini'))
conn = psycopg2.connect(**dict(config['database']))
print('PostgreSQL connection successful')
conn.close()
"
```

## 📊 Expected Results

### ✅ Successful Deletion
```
✅ PostgreSQL records deleted: 15
✅ Elasticsearch documents masked: 42
✅ ClickHouse records masked: 8
✅ S3 files deleted: 3
✅ Redis sessions cleared: 12
```

### ✅ Verification Success
```
✅ Found 42 documents with DELETED_USER in Elasticsearch
✅ Found 8 records with DELETED_USER in ClickHouse
✅ Contributors successfully kicked out (fetch API failed as expected)
```

## ⚠️ Common Issues

### 1. ClickHouse Connection Failed
```bash
# Check port-forwarding
kubectl get pods | grep clickhouse
kubectl port-forward clickhouse-0 8123:8123 9000:9000
```

### 2. Elasticsearch Timeout
```bash
# Check health
curl -s "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/_cluster/health"
```

### 3. PostgreSQL Connection Error
```bash
# Test connection
psql -h your_host -U your_username -d your_database
```

## 🎯 CSV Format

Your CSV file should have these columns:
```csv
contributor_id,email_address,name,team_id,status,last_active,project_count,active_project_count,inactive_project_count
4369b66e-2e37-46d3-98ee-57378a7d310f,pjkQyG@appen.com,zARt,,ACTIVE,2022-07-25 04:11:43,1,0,0
91bacc09-7146-4579-b0fc-16287064157d,dUkakn@appen.com,iqyO,,ACTIVE,2022-07-25 04:11:43,1,0,0
```

## 🚨 Safety Reminders

1. **ALWAYS run `--dry-run` first**
2. **Test with small datasets** (`--sample-size 3`)
3. **Verify connectivity** before running scripts
4. **Check logs** for errors and warnings
5. **Backup data** (scripts create automatic backups)

## 📞 Support

- Check logs in the current directory
- Verify configuration files
- Test individual components
- Use debug mode: `export PYTHONPATH=.`
