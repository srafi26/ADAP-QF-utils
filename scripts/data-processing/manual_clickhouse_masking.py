#!/usr/bin/env python3
"""
Manual ClickHouse Masking Script for Daily Report Tables

This script manually masks contributor data in ClickHouse tables, specifically
for CONTRIBUTOR_DAILY_REPORT_DOWNLOAD and CONTRIBUTOR_DAILY_REPORT_DOWNLOAD_SPECIFIC tables.

Usage:
    python manual_clickhouse_masking.py --contributor-id "contributor_id" --email "email@example.com"
    python manual_clickhouse_masking.py --contributor-id "contributor_id" --email "email@example.com" --clickhouse-url "http://localhost:8123"
"""

import argparse
import logging
import subprocess
import time
import sys
from typing import List, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def mask_contributor_in_clickhouse(contributor_id: str, email: str, clickhouse_url: str = None):
    """Manually mask contributor data in ClickHouse daily report tables"""
    
    # Default ClickHouse URL for integration
    if not clickhouse_url:
        clickhouse_url = "http://localhost:8123"
    
    logger.info(f"🎯 Masking contributor {contributor_id} ({email}) in ClickHouse daily report tables")
    logger.info(f"🌐 ClickHouse URL: {clickhouse_url}")
    
    # Define ClickHouse tables that contain contributor data
    # Based on actual ClickHouse schema analysis - verified tables that can be updated
    clickhouse_tables = [
        'kepler.unit_metrics',           # Main metrics table with contributor_id and email
        'kepler.unit_metrics_hourly',    # Aggregated hourly data with contributor_id and email
        'kepler.unit_metrics_topic',     # Kafka topic table (read-only, but may contain data)
        'kepler.accrued_contributor_stats' # Contributor stats table with email column
    ]
    
    masked_records = 0
    
    for table in clickhouse_tables:
        try:
            logger.info(f"📊 Processing ClickHouse table: {table}")
            
            # Create ClickHouse UPDATE query to mask contributor data
            # ClickHouse uses ALTER TABLE ... UPDATE syntax
            # NOTE: contributor_id is a key column and CANNOT be updated
            # Only email columns can be masked
            if 'accrued_contributor_stats' in table:
                # This table only has email column, no contributor_id
                update_query = f"""
                ALTER TABLE {table} 
                UPDATE 
                    email = 'deleted_user@deleted.com'
                WHERE 
                    email = '{email}'
                """
            else:
                # Tables with contributor_id (but we can't update the key column)
                update_query = f"""
                ALTER TABLE {table} 
                UPDATE 
                    email = 'deleted_user@deleted.com'
                WHERE 
                    contributor_id = '{contributor_id}' 
                    OR email = '{email}'
                """
            
            logger.info(f"📝 ClickHouse UPDATE Query for table {table}:")
            logger.info(f"   Table: {table}")
            logger.info(f"   Contributor ID: {contributor_id}")
            logger.info(f"   Email: {email}")
            logger.debug(f"Full query: {update_query}")
            
            # Execute ClickHouse query using curl
            curl_cmd = [
                'curl', '-s', '-X', 'POST',
                f'{clickhouse_url}/',
                '-H', 'Content-Type: text/plain',
                '-d', update_query
            ]
            
            logger.info(f"🚀 Executing ClickHouse curl command:")
            logger.info(f"   Command: {' '.join(curl_cmd)}")
            logger.info(f"   Target: {table}")
            logger.info(f"   Operation: ALTER TABLE UPDATE with masking")
            
            start_time = time.time()
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=60)
            elapsed_time = time.time() - start_time
            
            logger.info(f"⏱️  ClickHouse command completed in {elapsed_time:.2f}s with return code: {result.returncode}")
            
            # Log complete curl response
            logger.info(f"📋 Complete ClickHouse Curl Response:")
            logger.info(f"   Return Code: {result.returncode}")
            logger.info(f"   STDOUT: {result.stdout}")
            logger.info(f"   STDERR: {result.stderr}")
            
            if result.returncode == 0:
                logger.info(f"✅ ClickHouse update successful for table {table}")
                logger.info(f"📋 ClickHouse Response Data:")
                logger.info(f"   {result.stdout}")
                masked_records += 1
            else:
                # Check if it's a table not found error
                if "doesn't exist" in result.stdout or "Unknown table" in result.stdout:
                    logger.warning(f"⚠️  ClickHouse table {table} doesn't exist - skipping")
                    logger.info(f"   This is normal if the table hasn't been created yet")
                else:
                    logger.error(f"❌ ClickHouse update failed for table {table}")
                    logger.error(f"   Return code: {result.returncode}")
                    logger.error(f"   Error: {result.stderr}")
                    
        except subprocess.TimeoutExpired:
            logger.error(f"⏰ ClickHouse command timed out for table {table}")
        except Exception as e:
            logger.error(f"💥 Unexpected error processing table {table}: {e}")
    
    logger.info(f"🎭 ClickHouse masking completed for contributor {contributor_id}")
    logger.info(f"📊 Successfully processed {masked_records} tables")
    
    return masked_records

def main():
    parser = argparse.ArgumentParser(description='Manually mask contributor data in ClickHouse daily report tables')
    parser.add_argument('--contributor-id', required=True, help='Contributor ID to mask')
    parser.add_argument('--email', required=True, help='Contributor email to mask')
    parser.add_argument('--clickhouse-url', help='ClickHouse URL (default: http://localhost:8123)')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    if args.dry_run:
        logger.info("🔍 DRY RUN MODE - No actual changes will be made")
        logger.info(f"Would mask contributor: {args.contributor_id} ({args.email})")
        logger.info(f"Would use ClickHouse URL: {args.clickhouse_url or 'http://localhost:8123'}")
        return
    
    try:
        masked_count = mask_contributor_in_clickhouse(
            contributor_id=args.contributor_id,
            email=args.email,
            clickhouse_url=args.clickhouse_url
        )
        
        if masked_count > 0:
            logger.info(f"✅ Successfully masked contributor data in {masked_count} ClickHouse tables")
        else:
            logger.warning("⚠️  No ClickHouse tables were successfully processed")
            
    except Exception as e:
        logger.error(f"💥 Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
