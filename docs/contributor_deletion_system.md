# Contributor Deletion System Documentation

## Overview

The Contributor Deletion System is a comprehensive solution for securely removing contributor data across multiple data stores (PostgreSQL, Elasticsearch, and ClickHouse) while maintaining data integrity and compliance requirements.

## System Architecture

### Core Components

1. **`contributor_deletion.py`** - Main orchestration script
2. **`database_connections.py`** - PostgreSQL connection and project mapping
3. **`elasticsearch_masking.py`** - Elasticsearch data masking with defensive normalization
4. **`clickhouse_masking.py`** - ClickHouse data masking
5. **`unified_unit_routing_and_testing_script.py`** - API access verification

### Data Flow

```
Contributor ID + Email
        ↓
1. Project Mapping (PostgreSQL)
        ↓
2. PostgreSQL Deletion & Masking
        ↓
3. Elasticsearch Masking (Multi-phase)
        ↓
4. ClickHouse Masking
        ↓
5. Verification
```

## Usage

### Basic Command Structure

```bash
# Dry run (recommended first)
python3 scripts/api-testing/contributor_deletion.py --contributor-id <ID> --email <EMAIL> --config ~/config_integration.ini --dry-run

# Live execution
python3 scripts/api-testing/contributor_deletion.py --contributor-id <ID> --email <EMAIL> --config ~/config_integration.ini --execute
```

### Successful Examples

**MHUHMB@appen.com (Latest - 2025-09-25):**
```bash
cd /Users/srafi/ADAP-QF-utils && python3 scripts/api-testing/contributor_deletion.py --contributor-id 0b5adcd5-1a5f-424e-a9d2-bb312caacac7 --email MHUHMB@appen.com --config ~/config_integration.ini --execute
```

**TlDSJI@appen.com:**
```bash
cd /Users/srafi/ADAP-QF-utils && python3 scripts/api-testing/contributor_deletion.py --contributor-id c716bbd2-7035-4aa5-ba10-3fb5f3086c9f --email TlDSJI@appen.com --config ~/config_integration.ini --execute
```

## Technical Features

### 1. Dynamic Project Discovery

The system automatically discovers all project associations for a contributor by querying:
- `kepler_proj_job_contributor_t` - Direct job-project mappings
- `kepler_distribution_segment_t0` through `kepler_distribution_segment_t9` - Sharded distribution tables
- Additional relationship tables

### 2. Elasticsearch Masking with Defensive Normalization

**Problem Solved:** The "source vs fields/doc values" mismatch in Elasticsearch where data can be stored as strings, pipe-separated strings, or arrays in `_source`, but appear differently in search results.

**Solution:** Defensive normalization helpers in Painless scripts:
- `toList()` - Converts any data type to mutable List<String>
- `fromList()` - Preserves original data shape
- `maskEmails()` - Safely replaces target emails
- `getChild()` - Safe nested field access

**Supported Fields:**
- `latest.workerEmail`, `latest.lastAnnotatorEmail`, `latest.lastReviewerEmail`
- `history.workerEmail`, `history.lastAnnotatorEmail`
- `earliest.workerEmail`, `earliest.lastAnnotatorEmail`
- Root-level email fields

### 3. Multi-Phase Elasticsearch Approach

**Phase 1:** Targeted fallback masking (unit-metrics index)
**Phase 2:** Database-driven project index targeting
**Phase 3:** Dynamic discovery (if needed)

### 4. PostgreSQL Deletion & Masking

- Deactivates contributor records (sets status to 'DELETED')
- Masks PII data (email, name) with 'DELETED_USER' / 'deleted_user@deleted.com'
- Handles multiple contributor-related tables
- Maintains referential integrity

### 5. ClickHouse Masking

- Uses `replaceAll()` for precise email replacement in pipe-separated lists
- Targets multiple tables: `unit_metrics`, `unit_metrics_hourly`, `unit_metrics_topic`, `accrued_contributor_stats`
- Preserves other contributors' data in shared fields

## Configuration

### Required Configuration File

Create `~/config_integration.ini`:

```ini
[database]
host = kepler-pg-integration.cluster-ce52lgdtaew6.us-east-1.rds.amazonaws.com
port = 5432
name = kepler-app-db
user = kepler-app-db-id
password = 6g3evdSsVVErzGT0ALp7gGwYiccwmZSb

[elasticsearch]
host = https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com

[clickhouse]
host = localhost
port = 8123
user = kepler
password = C7pc0GmM79CNWzi0R3mt
```

## Verification Process

### Pre-Deletion Testing
```bash
python3 scripts/automation/unified_unit_routing_and_testing_script.py --contributor-id <ID> --email <EMAIL>
```

### Post-Deletion Verification
- Elasticsearch verification: Searches for unmasked emails
- API access testing: Confirms contributor can no longer access fetch/commit APIs
- Database verification: Checks PostgreSQL masking

## Success Criteria

✅ **PostgreSQL:** Records deactivated and PII masked
✅ **Elasticsearch:** All email fields masked across all project indices
✅ **ClickHouse:** Email addresses masked in all relevant tables
✅ **API Access:** Contributor receives SYSTEM_ERROR when attempting to access APIs
✅ **Verification:** No unmasked emails found in search results

## Recent Fixes (2025-09-25)

### Elasticsearch Painless Script Issues
- **Fixed:** `size/0` and `replace/2` errors
- **Fixed:** Mixed data type handling (strings vs arrays)
- **Fixed:** Nested field access (latest.workerEmail, history.workerEmail)
- **Added:** Defensive normalization helpers
- **Added:** Shape preservation when converting back to original types

### Dynamic Project Discovery
- **Fixed:** Hardcoded project ID removal
- **Added:** Database-driven project mapping
- **Added:** Sharded distribution table support
- **Added:** Comprehensive project association discovery

## Troubleshooting

### ClickHouse Authentication Issues
- **Issue:** Reports success but shows authentication errors
- **Status:** Known issue - ClickHouse credentials need verification
- **Workaround:** System continues to function despite auth errors

### Elasticsearch Task Completion
- **Issue:** Scripts hanging on task completion detection
- **Fixed:** Improved task status checking with both `task.completed` and top-level `completed` fields

### Project Index Discovery
- **Issue:** Missing project associations
- **Fixed:** Enhanced database queries including distribution segment tables
- **Added:** Dynamic shard count configuration

## Performance

- **Targeted Approach:** Database-driven targeting instead of scanning all 6,496 Elasticsearch indices
- **Efficient Processing:** Multi-phase approach with fallback mechanisms
- **Scalable:** Handles contributors with multiple project associations
- **Reliable:** Comprehensive error handling and verification

## Security & Compliance

- **PII Masking:** All personal information properly masked
- **Data Integrity:** Preserves other contributors' data in shared fields
- **Audit Trail:** Comprehensive logging of all operations
- **Verification:** Multiple verification steps ensure complete deletion

## Support

For issues or questions:
1. Check logs for detailed operation information
2. Verify configuration file settings
3. Test with dry run first
4. Review verification results
5. Check database connectivity and permissions
