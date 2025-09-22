#!/usr/bin/env python3
"""
Simple Contributor Deletion Script
=================================

This script runs the complete contributor deletion process without requiring job URLs.
It fetches inactive contributors and deletes them.

Usage:
    # Dry run (safe to test)
    python3 run_deletion.py --config ~/config_integration.ini --integration --sample-size 5
    
    # Actual deletion
    python3 run_deletion.py --config ~/config_integration.ini --integration --execute --sample-size 10
"""

import argparse
import os
import sys
import logging
from datetime import datetime

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

import subprocess
import tempfile
from delete_contributors_csv import CSVContributorDeleter

def fetch_inactive_contributors(config_file, integration, sample_size=10):
    """Fetch inactive contributors by running the fetch script"""
    logger = logging.getLogger(__name__)
    
    # Create a temporary CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_file = f"backups/inactive_contributors_{timestamp}.csv"
    
    # Ensure backups directory exists
    os.makedirs("backups", exist_ok=True)
    
    # Build command
    cmd = [
        "python3", "scripts/fetch_inactive_contributors.py",
        "--config", config_file,
        "--output", csv_file,
        "--days-inactive", "90",
        "--min-projects", "0"
    ]
    
    if integration:
        cmd.append("--integration")
    
    logger.info(f"Running fetch command: {' '.join(cmd)}")
    
    try:
        # Run the fetch script
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("✅ Successfully fetched inactive contributors")
            logger.info(f"📁 CSV file: {csv_file}")
            return csv_file
        else:
            logger.error(f"❌ Failed to fetch inactive contributors: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("❌ Fetch script timed out")
        return None
    except Exception as e:
        logger.error(f"❌ Error running fetch script: {e}")
        return None

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Run Contributor Deletion')
    parser.add_argument('--config', default=os.path.expanduser('~/config_integration.ini'),
                       help='Path to configuration file')
    parser.add_argument('--integration', action='store_true',
                       help='Use integration environment')
    parser.add_argument('--execute', action='store_true',
                       help='Actually perform deletion (without this, it\'s dry-run)')
    parser.add_argument('--skip-redis', action='store_true',
                       help='Skip Redis session clearing')
    parser.add_argument('--sample-size', type=int, default=10,
                       help='Number of contributors to process (default: 10)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting Contributor Deletion Process")
    logger.info(f"📁 Config: {args.config}")
    logger.info(f"🌍 Environment: {'Integration' if args.integration else 'Production'}")
    logger.info(f"⚙️  Mode: {'EXECUTE' if args.execute else 'DRY-RUN'}")
    logger.info(f"👥 Sample size: {args.sample_size}")
    
    try:
        # Step 1: Fetch inactive contributors
        logger.info("📋 Fetching inactive contributors...")
        csv_file = fetch_inactive_contributors(
            config_file=args.config,
            integration=args.integration,
            sample_size=args.sample_size
        )
        
        if not csv_file:
            logger.error("❌ Failed to fetch inactive contributors")
            return 1
        
        # Check if file exists (handle nested backups directory)
        if not os.path.exists(csv_file):
            # Try nested backups directory
            nested_path = os.path.join("backups", csv_file)
            if os.path.exists(nested_path):
                csv_file = nested_path
            else:
                logger.error(f"❌ CSV file not found: {csv_file}")
                return 1
        
        logger.info(f"✅ Fetched inactive contributors to: {csv_file}")
        
        # Step 2: Delete contributors
        logger.info("🗑️  Deleting contributors...")
        
        deleter = CSVContributorDeleter(
            config_file=args.config,
            integration=args.integration,
            skip_redis=args.skip_redis
        )
        
        # Load contributors from CSV
        contributors = deleter.load_contributors_from_csv(csv_file)
        logger.info(f"📊 Loaded {len(contributors)} contributors from CSV")
        
        if not contributors:
            logger.warning("⚠️  No contributors to delete")
            return 0
        
        # Execute deletion
        if args.execute:
            logger.info("🔥 EXECUTING DELETION...")
            deleter.execute_deletion(contributors)
        else:
            logger.info("🔍 DRY RUN - No actual deletion performed")
            deleter.execute_deletion(contributors)
        
        logger.info("✅ Deletion process completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Deletion failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())
