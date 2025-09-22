# 🚀 Contributor Deletion System - Comprehensive Usage Guide

## 📋 Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [System Architecture](#system-architecture)
4. [Scripts Overview](#scripts-overview)
5. [Configuration Setup](#configuration-setup)
6. [Usage Examples](#usage-examples)
7. [Testing Workflows](#testing-workflows)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

## 🎯 Overview

The Contributor Deletion System is a comprehensive solution for safely removing contributor data from multiple data sources while preserving reporting functionality. It performs PII masking instead of complete deletion to maintain data integrity for analytics and reporting.

### Key Features:
- **Multi-System Support**: PostgreSQL, Elasticsearch, ClickHouse, S3, Redis
- **PII Masking**: Replaces sensitive data with "DELETED_USER" to preserve reporting
- **Comprehensive Testing**: Full workflow testing with unit routing and API validation
- **Safety First**: Dry-run mode and extensive logging for safe operations
- **Integration Ready**: Works with both development and integration environments

## 🔧 Prerequisites

### 1. System Requirements
```bash
# Python 3.8+ with required packages
pip install -r requirements.txt

# Required Python packages:
# - psycopg2-binary (PostgreSQL)
# - redis (Redis)
# - boto3 (AWS S3)
# - requests (HTTP APIs)
# - configparser (Configuration)
```

### 2. Environment Access
- **PostgreSQL**: Database access with read/write permissions
- **Elasticsearch**: HTTP API access to search and update indices
- **ClickHouse**: HTTP API access for analytics data
- **S3**: AWS credentials for file operations
- **Redis**: Access for session management
- **Kubernetes**: For port-forwarding (integration environment)

### 3. Configuration Files
- `~/config.ini` (development environment)
- `~/config_integration.ini` (integration environment)

### 4. Network Access
- ClickHouse port-forwarding: `kubectl port-forward clickhouse-0 8123:8123 9000:9000`
- Elasticsearch endpoint access
- PostgreSQL database connectivity

## 🏗️ System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Contributor Deletion System                  │
├─────────────────────────────────────────────────────────────────┤
│  Input: CSV File with Contributor Data                          │
│  ↓                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │   PostgreSQL    │  │  Elasticsearch  │  │   ClickHouse    │ │
│  │   (Source of    │  │  (Unit View &   │  │  (Analytics &   │ │
│  │    Truth)       │  │   Judgment      │  │   Reporting)    │ │
│  │                 │  │     View)       │  │                 │ │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘ │
│  ↓                                                              │
│  ┌─────────────────┐  ┌─────────────────┐                      │
│  │      S3         │  │     Redis       │                      │
│  │  (File Storage) │  │   (Sessions)    │                      │
│  └─────────────────┘  └─────────────────┘                      │
│  ↓                                                              │
│  Output: Masked Data with "DELETED_USER" for Reporting          │
└─────────────────────────────────────────────────────────────────┘
```

## 📜 Scripts Overview

### 1. `comprehensive_testing_workflow.py` (Main Workflow)
**Purpose**: Complete end-to-end testing workflow
**Features**:
- Unit routing to jobs
- Pre-deletion API testing
- Contributor deletion
- Post-deletion verification
- Elasticsearch/ClickHouse validation

### 2. `scripts/delete_contributors_csv.py` (Core Deletion)
**Purpose**: Main contributor deletion script
**Features**:
- Multi-system deletion/masking
- Comprehensive logging
- Dry-run mode
- Error handling and recovery

### 3. `scripts/fetch_inactive_contributors.py` (Data Fetching)
**Purpose**: Fetch inactive contributors from database
**Features**:
- PostgreSQL querying
- CSV export
- Configurable criteria

### 4. `scripts/test_fetch_commit_apis.py` (API Testing)
**Purpose**: Test fetch/commit APIs for contributors
**Features**:
- Pre/post deletion testing
- Access validation
- Error detection

### 5. `setup_clickhouse_env.sh` (Environment Setup)
**Purpose**: Configure ClickHouse environment
**Features**:
- Environment variable setup
- Connection testing
- Port-forwarding guidance

## ⚙️ Configuration Setup

### 1. Create Configuration Files

#### `~/config.ini` (Development)
```ini
[database]
host = localhost
port = 5432
database = kepler_dev
username = your_username
password = your_password

[redis]
host = localhost
port = 6379
password = your_redis_password

[elasticsearch]
url = http://localhost:9200

[s3]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
bucket_name = your_bucket

[clickhouse]
username = kepler
password = your_password
host = localhost
port = 8123
```

#### `~/config_integration.ini` (Integration)
```ini
[database]
host = your_integration_db_host
port = 5432
database = kepler_integration
username = your_username
password = your_password

[redis]
host = your_integration_redis_host
port = 6379
password = your_redis_password

[elasticsearch]
url = https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com

[s3]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
aws_session_token = your_session_token
bucket_name = kepler-integration-bucket

[clickhouse]
username = kepler
password = cLE8L3OEdr63
host = localhost
port = 8123
```

### 2. Set Environment Variables
```bash
# ClickHouse credentials
export CLICKHOUSE_USERNAME='kepler'
export CLICKHOUSE_PASSWORD='cLE8L3OEdr63'

# Elasticsearch URL
export ELASTICSEARCH_URL='https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com'

# S3 credentials
export AWS_ACCESS_KEY_ID='your_access_key'
export AWS_SECRET_ACCESS_KEY='your_secret_key'
export AWS_SESSION_TOKEN='your_session_token'
export S3_BUCKET_NAME='kepler-integration-bucket'
```

### 3. Setup ClickHouse Port-Forwarding
```bash
# For integration environment
kubectl port-forward clickhouse-0 8123:8123 9000:9000

# Verify connectivity
curl -s "http://localhost:8123/ping"
```

## 🚀 Usage Examples

### 1. Complete Testing Workflow

#### Basic Usage
```bash
python3 comprehensive_testing_workflow.py \
  --project-id "fe92bf1f-46c3-4ebb-a09e-0557237f41e6" \
  --job-url "https://client.appen.com/quality/jobs/27f196f6-ee10-49ae-b8d1-8b983cd6f2ea?secret=mnbCcSEUWCwownwiLkv0z7iZ7raK3JbqlSCBzFGqDWMGCO" \
  --csv "backups/inactive_contributors_10.csv" \
  --config "~/config_integration.ini"
```

#### What This Does:
1. **Unit Routing**: Routes units to the specified job
2. **Pre-Deletion Testing**: Tests fetch/commit APIs
3. **Contributor Deletion**: Deletes contributors from all systems
4. **Post-Deletion Testing**: Verifies contributors are kicked out
5. **Verification**: Confirms "DELETED_USER" appears in data sources

### 2. Fetch Inactive Contributors

#### Dry Run (Safe)
```bash
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --dry-run
```

#### Execute (Create CSV)
```bash
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --execute
```

#### Custom Criteria
```bash
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --days-inactive 30 \
  --max-contributors 100 \
  --execute
```

### 3. Delete Contributors from CSV

#### Dry Run (Recommended First)
```bash
python3 scripts/delete_contributors_csv.py \
  --csv "backups/inactive_contributors_10.csv" \
  --config ~/config_integration.ini \
  --integration \
  --dry-run
```

#### Execute Deletion
```bash
python3 scripts/delete_contributors_csv.py \
  --csv "backups/inactive_contributors_10.csv" \
  --config ~/config_integration.ini \
  --integration \
  --execute
```

### 4. Test Fetch/Commit APIs

#### Test Before Deletion
```bash
python3 scripts/test_fetch_commit_apis.py \
  --csv "backups/inactive_contributors_10.csv" \
  --job-url "https://client.appen.com/quality/jobs/27f196f6-ee10-49ae-b8d1-8b983cd6f2ea?secret=mnbCcSEUWCwownwiLkv0z7iZ7raK3JbqlSCBzFGqDWMGCO" \
  --config ~/config_integration.ini \
  --integration \
  --sample-size 3
```

#### Test After Deletion (Should Fail)
```bash
python3 scripts/test_fetch_commit_apis.py \
  --csv "backups/inactive_contributors_10.csv" \
  --job-url "https://client.appen.com/quality/jobs/27f196f6-ee10-49ae-b8d1-8b983cd6f2ea?secret=mnbCcSEUWCwownwiLkv0z7iZ7raK3JbqlSCBzFGqDWMGCO" \
  --config ~/config_integration.ini \
  --integration \
  --sample-size 3 \
  --fetch-only
```

## 🧪 Testing Workflows

### 1. Complete End-to-End Test
```bash
# Step 1: Fetch inactive contributors
python3 scripts/fetch_inactive_contributors.py \
  --config ~/config_integration.ini \
  --integration \
  --execute

# Step 2: Run complete workflow
python3 comprehensive_testing_workflow.py \
  --project-id "your-project-id" \
  --job-url "your-job-url" \
  --csv "inactive_contributors_$(date +%Y%m%d_%H%M%S).csv" \
  --config ~/config_integration.ini
```

### 2. Individual Component Testing
```bash
# Test ClickHouse connectivity
source setup_clickhouse_env.sh
curl -s "http://localhost:8123/ping"

# Test Elasticsearch connectivity
curl -s "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/_cluster/health"

# Test PostgreSQL connectivity
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

### 3. Verification Testing
```bash
# Verify Elasticsearch masking
curl -s -X GET "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/project-your-project-id/_search" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "term": {
        "latest.workerEmail": "DELETED_USER"
      }
    }
  }'

# Verify ClickHouse masking
curl -s -X POST "http://localhost:8123/" \
  -H "Content-Type: text/plain" \
  -d "SELECT COUNT(*) FROM kepler.unit_metrics WHERE email = 'DELETED_USER'"
```

## 🔍 Troubleshooting

### Common Issues and Solutions

#### 1. ClickHouse Connection Failed
```bash
# Check port-forwarding
kubectl get pods | grep clickhouse
kubectl port-forward clickhouse-0 8123:8123 9000:9000

# Test connectivity
curl -s "http://localhost:8123/ping"
```

#### 2. Elasticsearch Timeout
```bash
# Check Elasticsearch health
curl -s "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/_cluster/health"

# Increase timeout in script (modify timeout parameter)
```

#### 3. PostgreSQL Connection Error
```bash
# Verify credentials in config file
cat ~/config_integration.ini

# Test connection manually
psql -h your_host -U your_username -d your_database
```

#### 4. S3 Access Denied
```bash
# Check AWS credentials
aws sts get-caller-identity

# Verify S3 bucket permissions
aws s3 ls s3://your-bucket-name
```

#### 5. Redis Connection Failed
```bash
# Check Redis service
redis-cli -h your_redis_host -p 6379 ping

# Verify credentials
redis-cli -h your_redis_host -p 6379 -a your_password ping
```

### Debug Mode
```bash
# Enable debug logging
export PYTHONPATH=.
python3 -c "
import logging
logging.basicConfig(level=logging.DEBUG)
# Run your script here
"
```

## 📋 Best Practices

### 1. Safety First
- **Always run dry-run first**: `--dry-run` flag
- **Test with small datasets**: Use `--sample-size` for testing
- **Backup data**: Scripts automatically create CSV backups
- **Monitor logs**: Check log files for errors and warnings

### 2. Environment Management
- **Use integration environment**: Test in integration before production
- **Verify connectivity**: Test all connections before running scripts
- **Check port-forwarding**: Ensure ClickHouse port-forwarding is active

### 3. Data Validation
- **Verify CSV format**: Ensure contributor_id and email_address columns exist
- **Check data quality**: Validate contributor IDs and email addresses
- **Monitor results**: Verify "DELETED_USER" appears in data sources

### 4. Performance Optimization
- **Batch processing**: Process contributors in batches for large datasets
- **Parallel execution**: Scripts use threading for better performance
- **Resource monitoring**: Monitor system resources during execution

### 5. Error Handling
- **Graceful failures**: Scripts continue even if non-critical steps fail
- **Comprehensive logging**: All operations are logged with timestamps
- **Recovery procedures**: Failed operations can be retried

## 📊 Expected Outputs

### 1. Successful Deletion
```
✅ PostgreSQL records deleted: 15
✅ Elasticsearch documents masked: 42
✅ ClickHouse records masked: 8
✅ S3 files deleted: 3
✅ Redis sessions cleared: 12
```

### 2. Verification Results
```
✅ Found 42 documents with DELETED_USER in Elasticsearch
✅ Found 8 records with DELETED_USER in ClickHouse
✅ Contributors successfully kicked out (fetch API failed as expected)
```

### 3. Error Scenarios
```
⚠️  ClickHouse connection failed (expected if port-forward not active)
⚠️  S3 access denied (check AWS credentials)
❌ PostgreSQL connection failed (check database credentials)
```

## 🔄 Workflow Summary

1. **Setup**: Configure environment and credentials
2. **Fetch**: Get inactive contributors from database
3. **Test**: Verify contributors can access system
4. **Route**: Route units to jobs for testing
5. **Delete**: Remove/mask contributor data
6. **Verify**: Confirm deletion was effective
7. **Validate**: Check "DELETED_USER" appears in data sources

This comprehensive system ensures safe, thorough, and verifiable contributor deletion while preserving data integrity for reporting and analytics purposes.
