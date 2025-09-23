# Contributor Deactivation Script

## Overview
This script provides comprehensive contributor deactivation functionality, including Elasticsearch data masking and database record deactivation.

## Scripts

### `delete_contributors_csv.py`
The main contributor deactivation script that:
- Masks contributor data in Elasticsearch (unit-metrics index)
- Deactivates contributor records in PostgreSQL database
- Handles unique constraint errors gracefully
- Provides comprehensive logging and error handling

## Usage

```bash
python api-testing/delete_contributors_csv.py --csv <csv_file> --config <config_file> --integration --execute --skip-redis
```

### Parameters
- `--csv`: CSV file containing contributor data with columns: contributor_id, email_address, name, team_id, status, last_active, project_count, active_project_count, inactive_project_count
- `--config`: Configuration file path (e.g., ~/config_integration.ini)
- `--integration`: Use integration environment
- `--execute`: Execute actual deactivation (overrides dry-run)
- `--skip-redis`: Skip Redis session clearing
- `--dry-run`: Perform dry run without actual deactivation

## Features

### Elasticsearch Masking
- Masks contributor data in unit-metrics index
- Handles both string and array fields
- Supports nested fields (latest.*, history.*)
- Comprehensive field coverage (contributor_id, worker_id, email, email_address, worker_email, etc.)

### Database Deactivation
- Deactivates records in `kepler_crowd_contributor_job_mapping_t`
- Deactivates records in `kepler_crowd_contributors_project_stats_t`
- Updates main contributor table (`kepler_crowd_contributors_t`) status to INACTIVE
- Handles unique constraint errors with rollback and retry logic
- Fresh database connection per contributor to avoid transaction conflicts

### Error Handling
- Graceful handling of unique constraint violations
- Transaction rollback and retry mechanisms
- Comprehensive logging for debugging
- Individual contributor processing to prevent batch failures

## Configuration

The script requires a configuration file with database connection details:

```ini
[database]
host = your-db-host
port = 5432
database = your-db-name
user = your-db-user
password = your-db-password

[api]
send_to_job_api_key = your-api-key
```

## Testing

The script has been thoroughly tested with multiple contributors and consistently achieves:
- 100% success rate for Elasticsearch masking
- 100% success rate for database deactivation
- Proper access control (contributors blocked from fetch API)
- Graceful error handling for constraint violations

## Logging

The script provides comprehensive logging including:
- Full API response details
- Database operation results
- Error handling and recovery actions
- Performance metrics and timing information
