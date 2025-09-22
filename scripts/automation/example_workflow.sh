#!/bin/bash

# Example Workflow Script for Contributor Deletion System
# This script demonstrates the complete workflow from fetching to deletion

set -e  # Exit on any error

echo "🚀 Contributor Deletion System - Example Workflow"
echo "=================================================="

# Configuration
CONFIG_FILE="~/config_integration.ini"
PROJECT_ID="fe92bf1f-46c3-4ebb-a09e-0557237f41e6"
JOB_URL="https://client.appen.com/quality/jobs/27f196f6-ee10-49ae-b8d1-8b983cd6f2ea?secret=mnbCcSEUWCwownwiLkv0z7iZ7raK3JbqlSCBzFGqDWMGCO"
CSV_FILE="backups/inactive_contributors_$(date +%Y%m%d_%H%M%S).csv"

echo "📋 Configuration:"
echo "   Config File: $CONFIG_FILE"
echo "   Project ID: $PROJECT_ID"
echo "   Job URL: $JOB_URL"
echo "   CSV File: $CSV_FILE"
echo ""

# Step 1: Setup ClickHouse environment
echo "🔧 Step 1: Setting up ClickHouse environment..."
source setup_clickhouse_env.sh
echo "✅ ClickHouse environment setup complete"
echo ""

# Step 2: Test connectivity
echo "🔍 Step 2: Testing connectivity..."
echo "   Testing ClickHouse..."
if curl -s "http://localhost:8123/ping" > /dev/null; then
    echo "   ✅ ClickHouse: Connected"
else
    echo "   ❌ ClickHouse: Connection failed (check port-forwarding)"
    echo "   Run: kubectl port-forward clickhouse-0 8123:8123 9000:9000"
fi

echo "   Testing Elasticsearch..."
if curl -s "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/_cluster/health" > /dev/null; then
    echo "   ✅ Elasticsearch: Connected"
else
    echo "   ❌ Elasticsearch: Connection failed"
fi
echo ""

# Step 3: Fetch inactive contributors
echo "📥 Step 3: Fetching inactive contributors..."
echo "   Running dry-run first..."
python3 scripts/fetch_inactive_contributors.py \
    --config "$CONFIG_FILE" \
    --integration \
    --dry-run

echo ""
echo "   Executing fetch..."
python3 scripts/fetch_inactive_contributors.py \
    --config "$CONFIG_FILE" \
    --integration \
    --execute

# Find the generated CSV file
GENERATED_CSV=$(ls -t inactive_contributors_*.csv 2>/dev/null | head -1)
if [ -n "$GENERATED_CSV" ]; then
    CSV_FILE="$GENERATED_CSV"
    echo "   ✅ Generated CSV: $CSV_FILE"
else
    echo "   ⚠️  No CSV generated, using existing file"
    CSV_FILE="backups/inactive_contributors_10.csv"
fi
echo ""

# Step 4: Test APIs before deletion
echo "🧪 Step 4: Testing APIs before deletion..."
python3 scripts/test_fetch_commit_apis.py \
    --csv "$CSV_FILE" \
    --job-url "$JOB_URL" \
    --config "$CONFIG_FILE" \
    --integration \
    --sample-size 3
echo ""

# Step 5: Run deletion (dry-run first)
echo "🗑️  Step 5: Running deletion (dry-run first)..."
python3 scripts/delete_contributors_csv.py \
    --csv "$CSV_FILE" \
    --config "$CONFIG_FILE" \
    --integration \
    --dry-run

echo ""
echo "   Executing deletion..."
python3 scripts/delete_contributors_csv.py \
    --csv "$CSV_FILE" \
    --config "$CONFIG_FILE" \
    --integration \
    --execute
echo ""

# Step 6: Test APIs after deletion
echo "🔍 Step 6: Testing APIs after deletion (should fail)..."
python3 scripts/test_fetch_commit_apis.py \
    --csv "$CSV_FILE" \
    --job-url "$JOB_URL" \
    --config "$CONFIG_FILE" \
    --integration \
    --sample-size 3 \
    --fetch-only
echo ""

# Step 7: Verify masking
echo "✅ Step 7: Verifying masking..."
echo "   Checking Elasticsearch for DELETED_USER..."
curl -s -X GET "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com/project-$PROJECT_ID/_search" \
    -H "Content-Type: application/json" \
    -d '{
        "query": {
            "term": {
                "latest.workerEmail": "DELETED_USER"
            }
        },
        "size": 5
    }' | jq '.hits.total.value // 0'

echo "   Checking ClickHouse for DELETED_USER..."
curl -s -X POST "http://localhost:8123/" \
    -H "Content-Type: text/plain" \
    -d "SELECT COUNT(*) FROM kepler.unit_metrics WHERE email = 'DELETED_USER'"
echo ""

echo "🎉 Workflow completed!"
echo "====================="
echo "📊 Summary:"
echo "   CSV File: $CSV_FILE"
echo "   Project ID: $PROJECT_ID"
echo "   Check logs for detailed results"
echo ""
echo "🔍 To verify results manually:"
echo "   1. Check Elasticsearch: Search for 'DELETED_USER' in project indices"
echo "   2. Check ClickHouse: Query for 'DELETED_USER' in unit_metrics tables"
echo "   3. Check PostgreSQL: Verify contributor records are deleted/masked"
echo ""
echo "📝 Log files created:"
ls -la *.log 2>/dev/null || echo "   No log files found"
