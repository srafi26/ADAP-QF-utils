# ADAP-QF-utils Documentation

This directory contains comprehensive documentation for the ADAP Quality Framework utilities, with a focus on the contributor deletion system.

## 📚 Documentation Index

### Core Documentation
- **[COMPREHENSIVE_USAGE_GUIDE.md](COMPREHENSIVE_USAGE_GUIDE.md)** - Complete usage guide with detailed examples and troubleshooting
- **[COMPLETE_SEQUENCE_GUIDE.md](COMPLETE_SEQUENCE_GUIDE.md)** - Step-by-step sequence guide for contributor deletion
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Quick reference for common commands and operations

### System Documentation
- **[system-architecture.md](system-architecture.md)** - System architecture diagrams and component overview
- **[workflow-diagram.md](workflow-diagram.md)** - Detailed workflow diagrams for all processes

## 🎯 Quick Start

### For New Users
1. Start with [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for immediate usage
2. Read [COMPLETE_SEQUENCE_GUIDE.md](COMPLETE_SEQUENCE_GUIDE.md) for step-by-step instructions
3. Refer to [COMPREHENSIVE_USAGE_GUIDE.md](COMPREHENSIVE_USAGE_GUIDE.md) for detailed information

### For System Administrators
1. Review [system-architecture.md](system-architecture.md) for system overview
2. Study [workflow-diagram.md](workflow-diagram.md) for process understanding
3. Use [COMPREHENSIVE_USAGE_GUIDE.md](COMPREHENSIVE_USAGE_GUIDE.md) for configuration and troubleshooting

## 🔧 Script Categories

### API Testing Scripts (`scripts/api-testing/`)
- **`test_fetch_commit_apis.py`** - Test fetch and commit APIs for contributors
- **`delete_contributors_csv.py`** - Main contributor deletion script
- **`fetch_inactive_contributors.py`** - Fetch inactive contributors from database

### Data Processing Scripts (`scripts/data-processing/`)
- **`manual_elasticsearch_masking.py`** - Manual Elasticsearch data masking
- **`manual_clickhouse_masking.py`** - Manual ClickHouse data masking

### Automation Scripts (`scripts/automation/`)
- **`example_workflow.sh`** - Example workflow script
- **`unified_unit_routing_and_testing_script.py`** - Unit routing automation

## 📋 Configuration

### Example Configuration Files (`examples/config/`)
- **`config_example.ini`** - Development environment configuration template
- **`config_integration_example.ini`** - Integration environment configuration template

### Setup Scripts (`examples/`)
- **`setup_clickhouse_env.sh`** - ClickHouse environment setup script
- **`contributors_example.csv`** - Example CSV format for contributor data

## 🚀 Common Workflows

### 1. Simple Contributor Deletion
```bash
# Fetch inactive contributors
python scripts/api-testing/fetch_inactive_contributors.py --config ~/config_integration.ini --integration --execute

# Delete contributors (dry-run first!)
python scripts/api-testing/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --dry-run

# Execute deletion
python scripts/api-testing/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --execute
```

### 2. Unit Routing and Testing (Real Example)
```bash
# Combined unit routing and testing with real project data
python scripts/automation/unified_unit_routing_and_testing_script.py \
  --mode combined \
  --project-id "83d4a405-cd08-4895-af71-d8e6b7f953b2" \
  --csv "backups/single_contributor_test.csv" \
  --job-url "https://account.integration.cf3.us/quality/tasks/7e7e0b6d-c0ba-47e3-86a3-fd5e1b5dd468?secret=jFJFscUqSotzavqU7dIk8tk16kXgC7mtnoB8B8mXGFfCZU" \
  --single-unit

# Delete contributors with real configuration
python scripts/api-testing/delete_contributors_csv.py \
  --csv backups/single_contributor_test.csv \
  --config ~/config_integration.ini \
  --integration \
  --execute \
  --skip-redis
```

## 🔍 System Components

### Data Sources
- **PostgreSQL** - Source of truth for contributor data
- **Elasticsearch** - Unit View and Judgment View data
- **ClickHouse** - Analytics and reporting data
- **S3** - File storage for contributor data
- **Redis** - Session and cache management

### Key Features
- **PII Masking** - Replaces sensitive data with "DELETED_USER"
- **Dry Run Mode** - Safe testing without actual deletion
- **Comprehensive Logging** - Detailed operation tracking
- **Error Handling** - Graceful failure recovery
- **Backup Creation** - Automatic data backup before deletion

## 🚨 Safety Features

### Built-in Safety
- **Dry Run Mode** - Test operations without data loss
- **Confirmation Prompts** - User confirmation for destructive operations
- **Automatic Backups** - CSV backups created before deletion
- **Comprehensive Logging** - Full audit trail of operations
- **Error Recovery** - Graceful handling of failures

### Best Practices
- Always run `--dry-run` first
- Test with small datasets (`--sample-size 3`)
- Verify connectivity before running scripts
- Check logs for errors and warnings
- Use integration environment for testing

## 📊 Expected Results

### Successful Deletion
```
✅ PostgreSQL records deleted: 15
✅ Elasticsearch documents masked: 42
✅ ClickHouse records masked: 8
✅ S3 files deleted: 3
✅ Redis sessions cleared: 12
```

### Verification Success
```
✅ Found 42 documents with DELETED_USER in Elasticsearch
✅ Found 8 records with DELETED_USER in ClickHouse
✅ Contributors successfully kicked out (fetch API failed as expected)
```

## 🔧 Troubleshooting

### Common Issues
1. **ClickHouse Connection Failed** - Check port-forwarding
2. **Elasticsearch Timeout** - Verify cluster health
3. **PostgreSQL Connection Error** - Check credentials
4. **S3 Access Denied** - Verify AWS credentials
5. **Redis Connection Failed** - Check Redis service

### Debug Mode
```bash
# Enable debug logging
export PYTHONPATH=.
python -c "import logging; logging.basicConfig(level=logging.DEBUG)"
```

## 📞 Support

- Check logs in the current directory
- Verify configuration files
- Test individual components
- Review comprehensive usage guide for detailed troubleshooting
- Use debug mode for detailed error information

## 🎉 Ready to Use!

The system is now clean, organized, and ready for production use. All documentation is comprehensive and examples are provided to get started quickly.

**Start with the [QUICK_REFERENCE.md](QUICK_REFERENCE.md) for immediate usage, then refer to [COMPREHENSIVE_USAGE_GUIDE.md](COMPREHENSIVE_USAGE_GUIDE.md) for detailed information.**
