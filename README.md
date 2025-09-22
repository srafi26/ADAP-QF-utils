# ADAP-QF-utils

A collection of utility scripts and tools for ADAP Quality Framework (QF) operations and testing.

## Overview

This repository contains various utility scripts designed to support ADAP operations, API testing, data processing, and automation tasks. The scripts are organized by category to maintain clarity and ease of use.

## Repository Structure

```
ADAP-QF-utils/
├── scripts/
│   ├── api-testing/          # API testing and validation scripts
│   ├── data-processing/      # Data manipulation and processing tools
│   ├── automation/           # Automation and workflow scripts
│   └── monitoring/           # Monitoring and alerting utilities
├── docs/                     # Documentation and guides
├── examples/                 # Example configurations and usage
├── tests/                    # Test files and test data
├── requirements.txt          # Python dependencies
└── README.md                # This file
```

## 📚 Documentation

### Comprehensive Documentation
- **[docs/README.md](docs/README.md)** - Complete documentation index and overview
- **[docs/COMPREHENSIVE_USAGE_GUIDE.md](docs/COMPREHENSIVE_USAGE_GUIDE.md)** - Detailed usage guide with examples and troubleshooting
- **[docs/COMPLETE_SEQUENCE_GUIDE.md](docs/COMPLETE_SEQUENCE_GUIDE.md)** - Step-by-step sequence guide for contributor deletion
- **[docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)** - Quick reference for common commands and operations
- **[docs/system-architecture.md](docs/system-architecture.md)** - System architecture diagrams and component overview
- **[docs/workflow-diagram.md](docs/workflow-diagram.md)** - Detailed workflow diagrams for all processes

### Quick Start
1. **New Users**: Start with [docs/QUICK_REFERENCE.md](docs/QUICK_REFERENCE.md)
2. **Step-by-Step**: Follow [docs/COMPLETE_SEQUENCE_GUIDE.md](docs/COMPLETE_SEQUENCE_GUIDE.md)
3. **Detailed Info**: Read [docs/COMPREHENSIVE_USAGE_GUIDE.md](docs/COMPREHENSIVE_USAGE_GUIDE.md)

## Available Scripts

### API Testing Scripts (`scripts/api-testing/`)

#### `test_fetch_commit_apis.py`
Tests the fetch and commit APIs for contributors to verify they can access the system.

**Purpose:**
- Tests fetch API: `/dist/internal/fetch`
- Tests commit API: `/dist/internal/commit`
- Verifies if deleted contributors can still access the system
- Useful for testing contributor deletion system integrity

#### `delete_contributors_csv.py`
Comprehensive contributor deletion script that removes contributor data from multiple systems.

**Purpose:**
- Deletes contributors from PostgreSQL database
- Masks PII data in Elasticsearch (Unit View & Judgment View)
- Masks analytics data in ClickHouse
- Clears Redis sessions and caches
- Deletes S3 files associated with contributors
- Provides comprehensive logging and backup functionality

#### `fetch_inactive_contributors.py`
Fetches inactive contributors from the PostgreSQL database for deletion.

**Purpose:**
- Queries PostgreSQL for inactive contributors
- Exports results to CSV format
- Configurable criteria (days inactive, project count, etc.)
- Safe dry-run mode for testing

### Data Processing Scripts (`scripts/data-processing/`)

#### `manual_elasticsearch_masking.py`
Manual Elasticsearch data masking utility for specific contributor data.

#### `manual_clickhouse_masking.py`
Manual ClickHouse data masking utility for analytics data.

### Automation Scripts (`scripts/automation/`)

#### `comprehensive_testing_workflow.py`
Complete end-to-end testing workflow for contributor deletion.

**Purpose:**
- Unit routing to jobs
- Pre-deletion API testing
- Contributor deletion
- Post-deletion verification
- Elasticsearch/ClickHouse validation

#### `run_deletion.py`
Simple contributor deletion runner script.

**Purpose:**
- Fetches inactive contributors automatically
- Deletes from all data sources
- No job URLs required
- Easy-to-use interface

#### `run_deletion.sh`
Shell script wrapper for contributor deletion.

**Purpose:**
- Colored output
- Safety confirmations
- Error handling
- No job URLs required

#### `unified_unit_routing_and_testing_script.py`
Unit routing automation script for testing workflows.

**Usage:**
```bash
# Dry run to see what would be deleted
python scripts/api-testing/delete_contributors_csv.py --csv contributors.csv --config ~/config.ini --dry-run

# Execute actual deletion
python scripts/api-testing/delete_contributors_csv.py --csv contributors.csv --config ~/config.ini --execute

# Use integration environment
python scripts/api-testing/delete_contributors_csv.py --csv contributors.csv --config ~/config_integration.ini --integration --execute

# Skip Redis session clearing
python scripts/api-testing/delete_contributors_csv.py --csv contributors.csv --config ~/config.ini --execute --skip-redis
```

**Options:**
- `--csv`: CSV file containing contributor data (required)
- `--config`: Configuration file path (required)
- `--integration`: Use integration environment settings
- `--dry-run`: Perform dry run without actual deletion
- `--execute`: Execute actual deletion (overrides dry-run)
- `--skip-redis`: Skip Redis session clearing (optional)

**Usage:**
```bash
# Test with CSV file
python scripts/api-testing/test_fetch_commit_apis.py --csv contributors.csv --job-url "https://client.appen.com/quality/jobs/..."

# Test single contributor
python scripts/api-testing/test_fetch_commit_apis.py --contributor-id CONTRIB_123 --job-url "https://client.appen.com/quality/jobs/..."

# Test with commit option
python scripts/api-testing/test_fetch_commit_apis.py --csv contributors.csv --job-url "https://client.appen.com/quality/jobs/..." --commit-option "Yes"

# Dry run to see what would be tested
python scripts/api-testing/test_fetch_commit_apis.py --csv contributors.csv --job-url "https://client.appen.com/quality/jobs/..." --dry-run
```

**Options:**
- `--csv`: CSV file containing contributor data
- `--contributor-id`: Single contributor ID to test
- `--email`: Email address for single contributor test
- `--job-url`: Task URL with job ID and secret (required)
- `--base-url`: Base URL for APIs (default: http://localhost:9801)
- `--output`: Output CSV filename for test results
- `--sample-size`: Number of contributors to test (default: test all)
- `--commit-option`: Judgment option to commit (e.g., "Yes", "No")
- `--dry-run`: Show what would be tested without executing

**CSV Format:**
The CSV file should contain the following columns:
- `contributor_id`: Contributor/Worker ID
- `email_address`: Email address
- `name`: Name (optional)

**Output:**
- Detailed logs with timestamps
- Test results saved to CSV file
- Summary statistics of test outcomes

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Appen-International/ADAP-QF-utils.git
cd ADAP-QF-utils
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Make scripts executable (if needed):
```bash
chmod +x scripts/*/*.py
```

## Development

### Code Quality
This repository follows Python best practices:
- Code formatting with Black
- Linting with Flake8
- Type checking with MyPy
- Testing with Pytest

### Pre-commit Hooks
Install pre-commit hooks for automatic code quality checks:
```bash
pre-commit install
```

### Running Tests
```bash
pytest tests/
```

## Contributing

1. Create a feature branch from `main`
2. Add your utility script to the appropriate category directory
3. Update this README with documentation for your script
4. Add tests if applicable
5. Submit a pull request

### Script Guidelines

When adding new scripts, please follow these guidelines:

1. **Documentation**: Include comprehensive docstrings and comments
2. **Error Handling**: Implement proper error handling and logging
3. **CLI Interface**: Use argparse for command-line interfaces
4. **Logging**: Use the Python logging module for output
5. **Configuration**: Support configuration files when appropriate
6. **Testing**: Include test cases for complex logic

### Directory Organization

- **api-testing/**: Scripts for testing APIs, endpoints, and integrations
- **data-processing/**: Scripts for data manipulation, transformation, and analysis
- **automation/**: Scripts for automating workflows and repetitive tasks
- **monitoring/**: Scripts for monitoring systems, generating alerts, and health checks

## Examples

### Configuration Examples (`examples/config/`)
- **`config_example.ini`** - Development environment configuration template
- **`config_integration_example.ini`** - Integration environment configuration template

### Setup Scripts (`examples/`)
- **`setup_clickhouse_env.sh`** - ClickHouse environment setup script
- **`contributors_example.csv`** - Example CSV format for contributor data

### Example CSV for API Testing
```csv
contributor_id,email_address,name
CONTRIB_123,user1@example.com,John Doe
CONTRIB_456,user2@example.com,Jane Smith
```

### Example Job URL
```
https://account.integration.cf3.us/quality/tasks/TASK_ID?secret=SECRET_KEY
```

### Common Workflows

#### 1. Simple Contributor Deletion
```bash
# Fetch inactive contributors
python scripts/api-testing/fetch_inactive_contributors.py --config ~/config_integration.ini --integration --execute

# Delete contributors (dry-run first!)
python scripts/api-testing/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --dry-run

# Execute deletion
python scripts/api-testing/delete_contributors_csv.py --csv backups/inactive_contributors_*.csv --config ~/config_integration.ini --integration --execute
```

#### 2. Complete Testing Workflow
```bash
# Run complete workflow with verification
python scripts/automation/comprehensive_testing_workflow.py \
  --project-id "your-project-id" \
  --job-url "https://account.integration.cf3.us/quality/tasks/TASK_ID?secret=SECRET_KEY" \
  --csv "contributors.csv" \
  --config "~/config_integration.ini"
```

#### 3. Simple Deletion (No Job URLs Required)
```bash
# Using shell script (easiest)
./scripts/automation/run_deletion.sh --dry-run --sample-size 5
./scripts/automation/run_deletion.sh --execute --sample-size 10

# Using Python script
python scripts/automation/run_deletion.py --config ~/config_integration.ini --integration --execute --sample-size 10
```

#### 4. Unit Routing and Testing (Real Example)
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

## Troubleshooting

### Common Issues

1. **Connection Errors**: Ensure the base URL is correct and accessible
2. **Authentication Issues**: Verify job URL contains valid secret
3. **CSV Format Errors**: Check CSV headers match expected format
4. **Permission Errors**: Ensure scripts have proper execution permissions

### Logs
Scripts generate detailed logs with timestamps. Check log files for debugging information.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For questions or issues:
1. Check the documentation in the `docs/` directory
2. Review existing issues in the repository
3. Create a new issue with detailed information about your problem

## Changelog

### v1.0.0
- Initial release
- Added `test_fetch_commit_apis.py` for API testing
- Established repository structure and documentation
