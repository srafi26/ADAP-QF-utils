#!/usr/bin/env python3
"""
Base classes and data structures for contributor deletion system
"""

import logging
import configparser
import os
import threading
from dataclasses import dataclass
from typing import List, Optional, Dict
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class ContributorInfo:
    """Data class for contributor information"""
    contributor_id: str
    email_address: str
    name: Optional[str] = None

@dataclass
class DeletionStats:
    """Statistics for deletion operations"""
    total_contributors: int = 0
    successful_deletions: int = 0
    failed_deletions: int = 0
    redis_sessions_cleared: int = 0
    elasticsearch_docs_masked: int = 0
    s3_files_deleted: int = 0
    postgresql_records_deleted: int = 0
    clickhouse_records_masked: int = 0
    pii_masked_records: int = 0

class ThreadSafeCounter:
    """Thread-safe counter for tracking operations across multiple threads"""
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self, amount=1):
        with self._lock:
            self._value += amount
    
    def get_value(self):
        with self._lock:
            return self._value
    
    def reset(self):
        with self._lock:
            self._value = 0

class BaseContributorDeleter:
    """Base class for contributor deletion operations"""
    
    def __init__(self, config_file: str, integration: bool = False, dry_run: bool = True, skip_redis: bool = False):
        self.config_file = config_file
        self.integration = integration
        self.dry_run = dry_run
        self.skip_redis = skip_redis
        self.config = configparser.ConfigParser()
        self.config.read(os.path.expanduser(config_file))
        self.stats = DeletionStats()
        
        # Thread-safe counters for concurrent operations
        self.elasticsearch_counter = ThreadSafeCounter()
        self.clickhouse_counter = ThreadSafeCounter()
        self.postgresql_counter = ThreadSafeCounter()
        
        # Database connections
        self.postgres_conn = None
        self.redis_conn = None
        self.s3_client = None
        self.clickhouse_url = None
        
        # Contributor ID lists for batch operations
        self.contributor_ids: List[str] = []
        self.email_addresses: List[str] = []
        
        # Tables using contributor_id (based on actual kepler-app schema)
        # CRITICAL: kepler_crowd_contributor_job_mapping_t must be processed FIRST
        # This is the key table that validateContributorAndJob checks for ACTIVE status
        
        # Tables that have a 'status' column and can be deactivated
        self.tables_with_status = [
            'kepler_crowd_contributor_job_mapping_t',  # CRITICAL: Must be first - this prevents platform access
            'kepler_crowd_contributors_t',            # Main contributor table
            'kepler_crowd_contributor_group_mapping_t',
            'kepler_crowd_contributors_project_stats_t',
            'kepler_crowd_contributors_team_mapping_t',
            'kepler_crowd_file_t',
            'kepler_work_job_pins_t'
        ]
        
        # Tables that do NOT have a 'status' column - these will be handled differently
        self.tables_without_status = [
            'kepler_work_job_question_history_t',      # No status column - just log for audit
            'kepler_work_job_interlocking_deduct_t',   # No status column - just log for audit
            'kepler_unit_giveup_log'                   # No status column - just log for audit
        ]
        
        # All tables combined for reference
        self.contributor_id_tables = self.tables_with_status + self.tables_without_status
    
    def load_contributors_from_csv(self, csv_file: str) -> List[ContributorInfo]:
        """Load contributors from CSV file"""
        import csv
        
        contributors = []
        invalid_rows = 0
        
        logger.info(f"Starting to load contributors from CSV file: {csv_file}")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                logger.info(f"CSV headers detected: {reader.fieldnames}")
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 since header is row 1
                    contributor = ContributorInfo(
                        contributor_id=row.get('contributor_id', '').strip(),
                        email_address=row.get('email_address', '').strip(),
                        name=row.get('name', '').strip() if row.get('name') else None
                    )
                    
                    if contributor.contributor_id and contributor.email_address:
                        contributors.append(contributor)
                        self.contributor_ids.append(contributor.contributor_id)
                        self.email_addresses.append(contributor.email_address)
                        logger.debug(f"Row {row_num}: Loaded contributor {contributor.contributor_id} ({contributor.email_address})")
                    else:
                        invalid_rows += 1
                        logger.warning(f"Row {row_num}: Skipping invalid row - missing contributor_id or email_address: {row}")
        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_file}")
            raise
        except Exception as e:
            logger.error(f"Error loading CSV file {csv_file}: {e}")
            raise
        
        self.stats.total_contributors = len(contributors)
        logger.info(f"CSV loading completed: {len(contributors)} valid contributors, {invalid_rows} invalid rows skipped")
        logger.info(f"Contributor IDs loaded: {len(self.contributor_ids)}")
        logger.info(f"Email addresses loaded: {len(self.email_addresses)}")
        
        return contributors
    
    def save_contributors_to_csv(self, contributors: List[ContributorInfo], filename: str = None) -> str:
        """Save contributors to CSV file for backup"""
        import csv
        
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"contributors_backup_{timestamp}.csv"
        
        filepath = os.path.expanduser(f"~/contributor-deletion-system-package/backups/{filename}")
        
        # Ensure backup directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        logger.info(f"Saving {len(contributors)} contributors to CSV: {filepath}")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['contributor_id', 'email_address', 'name']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            writer.writeheader()
            for contributor in contributors:
                writer.writerow({
                    'contributor_id': contributor.contributor_id,
                    'email_address': contributor.email_address,
                    'name': contributor.name or ''
                })
        
        logger.info(f"Successfully saved contributors to: {filepath}")
        return filepath
    
    def print_deletion_stats(self):
        """Print deletion statistics"""
        logger.info("=" * 60)
        logger.info("DELETION STATISTICS")
        logger.info("=" * 60)
        logger.info(f"📊 Total contributors processed: {self.stats.total_contributors}")
        logger.info(f"✅ Successful operations: {self.stats.successful_deletions}")
        logger.info(f"❌ Failed operations: {self.stats.failed_deletions}")
        logger.info("")
        logger.info("🔧 SYSTEM-SPECIFIC STATISTICS:")
        logger.info(f"   🔴 Redis sessions cleared: {self.stats.redis_sessions_cleared}")
        logger.info(f"   🔍 Elasticsearch documents masked: {self.stats.elasticsearch_docs_masked}")
        logger.info(f"   📊 ClickHouse records masked: {self.stats.clickhouse_records_masked}")
        logger.info(f"   📁 S3 files deleted: {self.stats.s3_files_deleted}")
        logger.info(f"   🗄️  PostgreSQL records deleted: {self.stats.postgresql_records_deleted}")
        logger.info(f"   🎭 PII masked records: {self.stats.pii_masked_records}")
        logger.info("")
        logger.info("📋 SUMMARY:")
        total_operations = (self.stats.redis_sessions_cleared + 
                          self.stats.elasticsearch_docs_masked + 
                          self.stats.clickhouse_records_masked +
                          self.stats.s3_files_deleted + 
                          self.stats.postgresql_records_deleted + 
                          self.stats.pii_masked_records)
        logger.info(f"   Total operations performed: {total_operations}")
        logger.info(f"   Success rate: {((self.stats.successful_deletions / max(self.stats.total_contributors, 1)) * 100):.1f}%")
        logger.info("=" * 60)
