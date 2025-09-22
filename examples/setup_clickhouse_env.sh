#!/bin/bash

# Setup ClickHouse Environment Variables
# This script sets up the ClickHouse credentials for the contributor deletion system

echo "🔧 Setting up ClickHouse environment variables..."

# Set ClickHouse credentials
export CLICKHOUSE_USERNAME='kepler'
export CLICKHOUSE_PASSWORD='cLE8L3OEdr63'

# Set ClickHouse connection details (adjust as needed)
export CLICKHOUSE_HOST='localhost'
export CLICKHOUSE_PORT='8123'

# Optional: Set ClickHouse database name
export CLICKHOUSE_DATABASE='default'

echo "✅ ClickHouse environment variables set:"
echo "   CLICKHOUSE_USERNAME: $CLICKHOUSE_USERNAME"
echo "   CLICKHOUSE_PASSWORD: [HIDDEN]"
echo "   CLICKHOUSE_HOST: $CLICKHOUSE_HOST"
echo "   CLICKHOUSE_PORT: $CLICKHOUSE_PORT"
echo "   CLICKHOUSE_DATABASE: $CLICKHOUSE_DATABASE"

echo ""
echo "🚀 You can now run the enhanced deletion script with ClickHouse support:"
echo "   python3 scripts/delete_contributors_csv.py --csv your_contributors.csv --config ~/config_integration.ini --integration --execute"
echo ""
echo "📊 Or test with dry run first:"
echo "   python3 scripts/delete_contributors_csv.py --csv your_contributors.csv --config ~/config_integration.ini --integration --dry-run"
echo ""
echo "🧪 Or test the enhanced masking capabilities:"
echo "   python3 test_enhanced_masking.py --contributor-id 'your-id' --email 'your-email@example.com' --config ~/config_integration.ini --dry-run"
echo ""
echo "ℹ️  Note: ClickHouse credentials are now stored in ~/config_integration.ini"
echo "   The script will automatically use these credentials when --integration flag is used."
echo ""
echo "🔧 IMPORTANT: ClickHouse Port-Forwarding Required"
echo "   Before running the deletion script, ensure ClickHouse port-forwarding is active:"
echo "   kubectl port-forward clickhouse-0 8123:8123 9000:9000"
echo ""
echo "🧪 Test ClickHouse connectivity:"
echo "   python3 test_clickhouse_connectivity.py --config ~/config_integration.ini"
