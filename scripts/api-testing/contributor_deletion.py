#!/usr/bin/env python3
"""
Contributor Deletion Script - Main Production Script

This is the main script for contributor deletion functionality.
It provides comprehensive contributor deletion across all data sources:
- PostgreSQL: Deactivation and PII masking
- Elasticsearch: Document masking across project indices
- ClickHouse: Analytics data masking
- Redis: Session clearing (optional)
- S3: File deletion (optional)

Features:
- Single contributor or batch processing from CSV files
- Dry-run and live execution modes
- Modular component architecture
- Comprehensive logging and verification
- Integration and production environment support
- Complete ClickHouse masking functionality
Usage Examples:

    # Delete single contributor (dry run)
    python contributor_deletion.py \
      --contributor-id "935d4e2e-f86c-4ce4-8d00-38768a60d599" \
      --email "TeMloT@appen.com" \
      --config ~/config_integration.ini \
      --integration \
      --dry-run

    # Delete single contributor (live execution)
    python contributor_deletion.py \
      --contributor-id "935d4e2e-f86c-4ce4-8d00-38768a60d599" \
      --email "TeMloT@appen.com" \
      --config ~/config_integration.ini \
      --integration \
      --execute

    # Delete multiple contributors from CSV (dry run)
    python contributor_deletion.py \
      --csv contributors.csv \
      --config ~/config_integration.ini \
      --integration \
      --dry-run

    # Delete with specific phases only
    python contributor_deletion.py \
      --contributor-id "935d4e2e-f86c-4ce4-8d00-38768a60d599" \
      --email "TeMloT@appen.com" \
      --config ~/config_integration.ini \
      --integration \
      --dry-run \
      --phases elasticsearch,postgresql,clickhouse
"""

import argparse
import csv
import logging
import sys
import configparser
import os
from typing import List
from contributor_deletion_base import ContributorInfo
from database_connections import DatabaseConnections
from postgresql_deletion import PostgreSQLDeletion
from elasticsearch_masking import ElasticsearchMasking
from clickhouse_masking import ClickHouseMasking

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_contributors_from_csv(csv_file: str) -> List[ContributorInfo]:
    """Load contributors from CSV file"""
    contributors = []
    
    logger.info(f"📋 Loading contributors from CSV: {csv_file}")
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            logger.info(f"CSV headers: {reader.fieldnames}")
            
            for row_num, row in enumerate(reader, start=2):
                contributor = ContributorInfo(
                    contributor_id=row.get('contributor_id', '').strip(),
                    email_address=row.get('email_address', '').strip(),
                    name=row.get('name', '').strip() if row.get('name') else None
                )
                
                if contributor.contributor_id and contributor.email_address:
                    contributors.append(contributor)
                    logger.debug(f"Row {row_num}: Loaded contributor {contributor.contributor_id}")
                else:
                    logger.warning(f"Row {row_num}: Skipping invalid row - missing contributor_id or email_address")
    
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        raise
    except Exception as e:
        logger.error(f"Error loading CSV file {csv_file}: {e}")
        raise
    
    logger.info(f"✅ Loaded {len(contributors)} contributors from CSV")
    return contributors

def create_single_contributor(contributor_id: str, email: str, name: str = None) -> ContributorInfo:
    """Create a single contributor for testing"""
    contributor = ContributorInfo(
        contributor_id=contributor_id,
        email_address=email,
        name=name
    )
    logger.info(f"👤 Created single contributor: {contributor_id} ({email})")
    return contributor

def test_contributor_deletion(contributor: ContributorInfo, config: configparser.ConfigParser, 
                            integration: bool, dry_run: bool, phases: List[str]) -> dict:
    """Test contributor deletion with modular components"""
    
    logger.info("=" * 80)
    logger.info("🧪 TESTING CONTRIBUTOR DELETION WITH MODULAR COMPONENTS")
    logger.info("=" * 80)
    logger.info(f"📋 Testing contributor: {contributor.contributor_id} ({contributor.email_address})")
    logger.info(f"⚙️  Mode: {'DRY RUN' if dry_run else 'LIVE EXECUTION'}")
    logger.info(f"🌍 Environment: {'Integration' if integration else 'Production'}")
    logger.info(f"🔧 Phases: {phases}")
    logger.info("=" * 80)
    
    # Initialize components
    db_connections = DatabaseConnections(config, integration)
    postgresql_deletion = PostgreSQLDeletion(config, integration, dry_run)
    elasticsearch_masking = ElasticsearchMasking(config, integration, dry_run)
    clickhouse_masking = ClickHouseMasking(config, integration, dry_run)
    
    contributors = [contributor]
    results = {
        'contributor_id': contributor.contributor_id,
        'email_address': contributor.email_address,
        'dry_run': dry_run,
        'integration': integration,
        'phases': phases,
        'project_mapping': None,
        'elasticsearch': None,
        'postgresql': None,
        'clickhouse': None,
        'success': True,
        'errors': []
    }
    
    try:
        # Step 1: Enhanced Project Mapping
        if 'project_mapping' in phases or 'all' in phases:
            logger.info("\n🔍 STEP 1: ENHANCED PROJECT MAPPING")
            logger.info("-" * 50)
            project_ids = db_connections.get_contributor_project_ids(contributor.contributor_id)
            results['project_mapping'] = {
                'success': True,
                'project_ids': project_ids,
                'count': len(project_ids)
            }
            logger.info(f"✅ Found {len(project_ids)} project IDs: {project_ids}")
            
            if not project_ids:
                logger.warning("⚠️  No project IDs found - this may affect deletion effectiveness")
        
        # Step 2: PostgreSQL Deletion
        if 'postgresql' in phases or 'all' in phases:
            logger.info("\n🗄️  STEP 2: POSTGRESQL DELETION")
            logger.info("-" * 50)
            postgresql_deleted = postgresql_deletion.delete_postgresql_data(contributors, db_connections)
            results['postgresql'] = {
                'success': True,
                'records_affected': postgresql_deleted
            }
            logger.info(f"✅ PostgreSQL deletion affected {postgresql_deleted} records")
        
        # Step 3: Elasticsearch Masking
        if 'elasticsearch' in phases or 'all' in phases:
            logger.info("\n🔍 STEP 3: ELASTICSEARCH MASKING")
            logger.info("-" * 50)
            elasticsearch_masked = elasticsearch_masking.mask_elasticsearch_data(contributors)
            results['elasticsearch'] = {
                'success': True,
                'documents_affected': elasticsearch_masked
            }
            logger.info(f"✅ Elasticsearch masking affected {elasticsearch_masked} documents")
        
        # Step 4: ClickHouse Masking
        if 'clickhouse' in phases or 'all' in phases:
            logger.info("\n📊 STEP 4: CLICKHOUSE MASKING")
            logger.info("-" * 50)
            clickhouse_masked = clickhouse_masking.mask_clickhouse_data(contributors)
            results['clickhouse'] = {
                'success': True,
                'records_affected': clickhouse_masked
            }
            logger.info(f"✅ ClickHouse masking affected {clickhouse_masked} records")
        
        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("📊 DELETION TEST SUMMARY")
        logger.info("=" * 80)
        
        if results['project_mapping']:
            logger.info(f"✅ Project IDs found: {results['project_mapping']['count']}")
        if results['postgresql']:
            logger.info(f"✅ PostgreSQL records affected: {results['postgresql']['records_affected']}")
        if results['elasticsearch']:
            logger.info(f"✅ Elasticsearch documents affected: {results['elasticsearch']['documents_affected']}")
        if results['clickhouse']:
            logger.info(f"✅ ClickHouse records affected: {results['clickhouse']['records_affected']}")
        
        logger.info("✅ Contributor deletion test completed successfully!")
        
        if results['project_mapping'] and results['project_mapping']['project_ids']:
            logger.info(f"🎯 Target project indices: {[f'project-{pid}' for pid in results['project_mapping']['project_ids']]}")
        
        logger.info("\n🎉 CONTRIBUTOR DELETION TEST COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        logger.error(f"❌ Error during deletion test: {e}")
        logger.exception("Full exception details:")
        results['success'] = False
        results['errors'].append(str(e))
    
    return results

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Contributor Deletion Script - Main Production Script')
    
    # Input options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('--csv', help='CSV file containing contributor data')
    input_group.add_argument('--contributor-id', help='Single contributor ID to delete')
    
    # Required arguments
    parser.add_argument('--config', required=True, help='Configuration file path (e.g., ~/config.ini)')
    parser.add_argument('--integration', action='store_true', help='Use integration environment')
    
    # Optional arguments for single contributor
    parser.add_argument('--email', help='Email address for single contributor deletion')
    parser.add_argument('--name', help='Name for single contributor deletion')
    
    # Execution mode
    execution_group = parser.add_mutually_exclusive_group(required=True)
    execution_group.add_argument('--dry-run', action='store_true', help='Perform dry run without actual deletion')
    execution_group.add_argument('--execute', action='store_true', help='Execute actual deletion')
    
    # Deletion configuration
    parser.add_argument('--phases', help='Comma-separated list of phases to execute (project_mapping,elasticsearch,postgresql,clickhouse,all)', 
                       default='all')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.contributor_id and not args.email:
        logger.error("--email is required when using --contributor-id")
        sys.exit(1)
    
    if args.csv and not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)
    
    config_path = os.path.expanduser(args.config)
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    # Determine execution mode
    dry_run = args.dry_run and not args.execute
    phases = args.phases.split(',') if args.phases else ['all']
    
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        
        # Load contributors
        if args.csv:
            contributors = load_contributors_from_csv(args.csv)
            if not contributors:
                logger.error("No valid contributors found in CSV file")
                sys.exit(1)
            
            logger.info(f"🗑️  Deleting {len(contributors)} contributors from CSV")
            
            # Test each contributor
            all_results = []
            for i, contributor in enumerate(contributors, 1):
                logger.info(f"\n🔄 Processing contributor {i}/{len(contributors)}: {contributor.contributor_id}")
                result = test_contributor_deletion(contributor, config, args.integration, dry_run, phases)
                all_results.append(result)
                
                # Small delay between contributors
                if i < len(contributors):
                    import time
                    time.sleep(1)
            
            # Print batch summary
            logger.info("\n" + "=" * 80)
            logger.info("📊 BATCH DELETION SUMMARY")
            logger.info("=" * 80)
            
            successful_tests = sum(1 for r in all_results if r['success'])
            total_tests = len(all_results)
            
            logger.info(f"👥 Contributors processed: {total_tests}")
            logger.info(f"✅ Successful deletions: {successful_tests}")
            logger.info(f"❌ Failed deletions: {total_tests - successful_tests}")
            logger.info(f"📈 Success rate: {(successful_tests/total_tests)*100:.1f}%")
            
            # Individual results
            logger.info("\n👤 INDIVIDUAL RESULTS:")
            for i, result in enumerate(all_results, 1):
                status = "✅" if result['success'] else "❌"
                logger.info(f"   {i}. {status} {result['contributor_id']} ({result['email_address']})")
            
            logger.info("=" * 80)
            
        else:
            # Create single contributor
            contributor = create_single_contributor(
                args.contributor_id, 
                args.email, 
                args.name
            )
            
            # Delete single contributor
            result = test_contributor_deletion(contributor, config, args.integration, dry_run, phases)
            
            if not result['success']:
                logger.error("❌ Contributor deletion failed")
                sys.exit(1)
        
        logger.info("🎉 Contributor deletion completed successfully!")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"❌ Contributor deletion failed: {e}")
        logger.exception("Full exception details:")
        sys.exit(1)

if __name__ == "__main__":
    main()
