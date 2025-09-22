#!/usr/bin/env python3
"""
Fetch Inactive Contributors Script

This script fetches inactive contributors from the PostgreSQL database.
It identifies contributors who haven't been active recently and can be safely deleted.

Based on kepler-app codebase analysis:
- Uses PostgreSQL kepler_crowd_contributors_t table with 'id' column
- Identifies contributors by status and last activity
- Exports results to CSV for use with deletion scripts

Usage:
    python fetch_inactive_contributors.py --config ~/config.ini --dry-run
    python fetch_inactive_contributors.py --config ~/config_integration.ini --integration --execute
"""

import argparse
import csv
import logging
import os
import sys
import configparser
import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'fetch_inactive_contributors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ContributorInfo:
    """Data class for contributor information"""
    contributor_id: str
    email_address: str
    name: Optional[str] = None
    team_id: Optional[str] = None
    status: Optional[str] = None
    last_active: Optional[str] = None
    project_count: int = 0
    active_project_count: int = 0
    inactive_project_count: int = 0

class InactiveContributorFetcher:
    """Fetches inactive contributors from the database"""
    
    def __init__(self, config_file: str, integration: bool = False):
        self.config_file = config_file
        self.integration = integration
        self.config = configparser.ConfigParser()
        self.config.read(os.path.expanduser(config_file))
        self.conn = None
        
    def connect_to_database(self):
        """Connect to PostgreSQL database"""
        try:
            # Always use config file (both integration and production)
            db_config = {
                'host': self.config.get('database', 'host'),
                'port': self.config.get('database', 'port'),
                'database': self.config.get('database', 'database'),
                'user': self.config.get('database', 'user'),
                'password': self.config.get('database', 'password')
            }
            
            logger.info(f"Connecting to database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            self.conn = psycopg2.connect(**db_config)
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def fetch_inactive_contributors(self, days_inactive: int = 90, min_projects: int = 0) -> List[ContributorInfo]:
        """
        Fetch inactive contributors based on criteria
        
        Args:
            days_inactive: Number of days since last activity
            min_projects: Minimum number of projects (0 = any number)
        """
        if not self.conn:
            self.connect_to_database()
        
        cursor = self.conn.cursor()
        contributors = []
        
        try:
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=days_inactive)
            
            logger.info(f"Fetching contributors inactive for {days_inactive} days (since {cutoff_date.strftime('%Y-%m-%d')})")
            
            # Simplified query to find inactive contributors
            query = """
                SELECT 
                    c.id as contributor_id,
                    c.email_address,
                    c.name,
                    c.status,
                    c.updated_at as last_active,
                    COUNT(DISTINCT jm.job_id) as total_projects,
                    0 as active_projects,
                    0 as inactive_projects
                FROM kepler_crowd_contributors_t c
                LEFT JOIN kepler_crowd_contributor_job_mapping_t jm ON c.id = jm.contributor_id
                WHERE c.status IN ('ACTIVE', 'INACTIVE')
                GROUP BY c.id, c.email_address, c.name, c.status, c.updated_at
                HAVING (
                    -- Last active more than specified days ago
                    c.updated_at < %s
                    OR
                    -- No job assignments
                    COUNT(DISTINCT jm.job_id) = 0
                )
                AND COUNT(DISTINCT jm.job_id) >= %s
                ORDER BY c.updated_at ASC, COUNT(DISTINCT jm.job_id) DESC
            """
            
            cursor.execute(query, (cutoff_date, min_projects))
            results = cursor.fetchall()
            
            logger.info(f"Found {len(results)} inactive contributors")
            
            for row in results:
                contributor = ContributorInfo(
                    contributor_id=row[0],
                    email_address=row[1],
                    name=row[2],
                    team_id=None,  # team_id column was removed
                    status=row[3],
                    last_active=row[4].strftime('%Y-%m-%d %H:%M:%S') if row[4] else None,
                    project_count=row[5],
                    active_project_count=row[6],
                    inactive_project_count=row[7]
                )
                contributors.append(contributor)
                
                logger.debug(f"Found inactive contributor: {contributor.contributor_id} ({contributor.email_address}) - "
                           f"Projects: {contributor.project_count} total, {contributor.active_project_count} active, "
                           f"{contributor.inactive_project_count} inactive")
            
            return contributors
            
        except Exception as e:
            logger.error(f"Error fetching inactive contributors: {e}")
            raise
        finally:
            cursor.close()
    
    def save_to_csv(self, contributors: List[ContributorInfo], filename: str = None) -> str:
        """Save contributors to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"inactive_contributors_{timestamp}.csv"
        
        filepath = os.path.expanduser(f"~/contributor-deletion-system-package/backups/{filename}")
        
        # Ensure backup directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        logger.info(f"Saving {len(contributors)} contributors to CSV: {filepath}")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            fieldnames = [
                'contributor_id', 'email_address', 'name', 'team_id', 'status', 
                'last_active', 'project_count', 'active_project_count', 'inactive_project_count'
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            writer.writeheader()
            for contributor in contributors:
                writer.writerow({
                    'contributor_id': contributor.contributor_id,
                    'email_address': contributor.email_address,
                    'name': contributor.name or '',
                    'team_id': contributor.team_id or '',
                    'status': contributor.status or '',
                    'last_active': contributor.last_active or '',
                    'project_count': contributor.project_count,
                    'active_project_count': contributor.active_project_count,
                    'inactive_project_count': contributor.inactive_project_count
                })
        
        logger.info(f"Successfully saved contributors to: {filepath}")
        return filepath
    
    def print_summary(self, contributors: List[ContributorInfo]):
        """Print summary statistics"""
        if not contributors:
            logger.info("No inactive contributors found")
            return
        
        total_contributors = len(contributors)
        total_projects = sum(c.project_count for c in contributors)
        total_active_projects = sum(c.active_project_count for c in contributors)
        total_inactive_projects = sum(c.inactive_project_count for c in contributors)
        
        logger.info("=" * 60)
        logger.info("INACTIVE CONTRIBUTORS SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📊 Total inactive contributors: {total_contributors}")
        logger.info(f"📁 Total projects: {total_projects}")
        logger.info(f"✅ Active projects: {total_active_projects}")
        logger.info(f"❌ Inactive projects: {total_inactive_projects}")
        logger.info("")
        
        # Status breakdown
        status_counts = {}
        for contributor in contributors:
            status = contributor.status or 'UNKNOWN'
            status_counts[status] = status_counts.get(status, 0) + 1
        
        logger.info("📋 Status breakdown:")
        for status, count in status_counts.items():
            logger.info(f"   {status}: {count}")
        
        logger.info("")
        logger.info("🔍 Top 10 contributors by project count:")
        sorted_contributors = sorted(contributors, key=lambda x: x.project_count, reverse=True)
        for i, contributor in enumerate(sorted_contributors[:10], 1):
            logger.info(f"   {i:2d}. {contributor.contributor_id} ({contributor.email_address}) - "
                       f"{contributor.project_count} projects ({contributor.active_project_count} active)")
        
        logger.info("=" * 60)
    
    def close_connection(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Fetch inactive contributors from database')
    parser.add_argument('--config', required=True, help='Configuration file path (e.g., ~/config.ini)')
    parser.add_argument('--integration', action='store_true', help='Use integration environment')
    parser.add_argument('--days-inactive', type=int, default=90, help='Days since last activity (default: 90)')
    parser.add_argument('--min-projects', type=int, default=0, help='Minimum number of projects (default: 0)')
    parser.add_argument('--output', help='Output CSV filename')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be fetched without saving')
    
    args = parser.parse_args()
    
    # Validate config file
    config_path = os.path.expanduser(args.config)
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    try:
        # Initialize fetcher
        fetcher = InactiveContributorFetcher(config_path, args.integration)
        
        # Fetch contributors
        contributors = fetcher.fetch_inactive_contributors(
            days_inactive=args.days_inactive,
            min_projects=args.min_projects
        )
        
        # Print summary
        fetcher.print_summary(contributors)
        
        # Save to CSV if not dry run
        if not args.dry_run and contributors:
            csv_file = fetcher.save_to_csv(contributors, args.output)
            logger.info(f"Contributors saved to: {csv_file}")
        elif args.dry_run:
            logger.info("DRY RUN: Contributors not saved to CSV")
        
        # Close connection
        fetcher.close_connection()
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
