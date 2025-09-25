#!/usr/bin/env python3
"""
Database connection management for contributor deletion system
"""

import os
import logging
import psycopg2
import redis
import boto3
from typing import List

logger = logging.getLogger(__name__)

# Configuration constants
DISTRIBUTION_SEGMENT_SHARD_COUNT = 10  # Number of sharded distribution segment tables (t0 through t9)

class DatabaseConnections:
    """Manages database connections for the contributor deletion system"""
    
    def __init__(self, config, integration: bool = False):
        self.config = config
        self.integration = integration
        
        # Connection objects
        self.postgres_conn = None
        self.redis_conn = None
        self.s3_client = None
        self.clickhouse_url = None
    
    def get_postgres_connection(self, force_fresh=False):
        """Get PostgreSQL connection"""
        if not self.postgres_conn or force_fresh:
            # Always use config file for database connection
            # The integration flag is used for other services (Redis, S3, etc.)
            db_config = {
                'host': self.config.get('database', 'host'),
                'port': self.config.get('database', 'port'),
                'database': self.config.get('database', 'database'),
                'user': self.config.get('database', 'user'),
                'password': self.config.get('database', 'password')
            }
            
            logger.info(f"🔗 Connecting to PostgreSQL: {db_config['host']}:{db_config['port']}/{db_config['database']}")
            logger.info(f"   Username: {db_config['user']}")
            
            self.postgres_conn = psycopg2.connect(**db_config)
            logger.info("✅ PostgreSQL connection established")
        return self.postgres_conn
    
    def get_redis_connection(self):
        """Get Redis connection"""
        if not self.redis_conn:
            if self.integration:
                # Use environment variables for integration
                redis_config = {
                    'host': os.getenv('REDIS_HOST', 'localhost'),
                    'port': int(os.getenv('REDIS_PORT', '6379')),
                    'password': os.getenv('REDIS_PASSWORD', ''),
                    'decode_responses': True
                }
            else:
                # Use config file
                redis_config = {
                    'host': self.config.get('redis_prod', 'host'),
                    'port': int(self.config.get('redis_prod', 'port')),
                    'password': self.config.get('redis_prod', 'password'),
                    'decode_responses': True
                }
            
            self.redis_conn = redis.Redis(**redis_config)
        return self.redis_conn
    
    def get_s3_client(self):
        """Get S3 client"""
        if not self.s3_client:
            if self.integration:
                # Use environment variables for integration
                s3_config = {
                    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
                    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
                    'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),
                    'region_name': 'us-east-1'
                }
            else:
                # Use config file
                s3_config = {
                    'aws_access_key_id': self.config.get('s3', 'aws_access_key_id'),
                    'aws_secret_access_key': self.config.get('s3', 'aws_secret_access_key'),
                    'aws_session_token': self.config.get('s3', 'aws_session_token'),
                    'region_name': 'us-east-1'
                }
            
            self.s3_client = boto3.client('s3', **s3_config)
        return self.s3_client
    
    def get_clickhouse_url(self):
        """Get ClickHouse connection URL with credentials"""
        if not self.clickhouse_url:
            if self.integration:
                # Use config file for integration environment
                try:
                    host = self.config.get('clickhouse', 'host')
                    port = self.config.get('clickhouse', 'port')
                    username = self.config.get('clickhouse', 'username')
                    password = self.config.get('clickhouse', 'password')
                    logger.info(f"🔗 ClickHouse config loaded from config file")
                except Exception as e:
                    logger.warning(f"⚠️  Could not load ClickHouse config from file: {e}")
                    # Fallback to environment variables
                    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
                    port = os.getenv('CLICKHOUSE_PORT', '8123')
                    username = os.getenv('CLICKHOUSE_USERNAME', 'kepler')
                    password = os.getenv('CLICKHOUSE_PASSWORD', 'cLE8L3OEdr63')
                    logger.info(f"🔗 ClickHouse config loaded from environment variables")
            else:
                # Use environment variables for production
                host = os.getenv('CLICKHOUSE_HOST', 'localhost')
                port = os.getenv('CLICKHOUSE_PORT', '8123')
                username = os.getenv('CLICKHOUSE_USERNAME', 'kepler')
                password = os.getenv('CLICKHOUSE_PASSWORD', 'cLE8L3OEdr63')
                logger.info(f"🔗 ClickHouse config loaded from environment variables")
            
            self.clickhouse_url = f"http://{username}:{password}@{host}:{port}"
            logger.info(f"🔗 ClickHouse URL configured: http://{username}:***@{host}:{port}")
            logger.debug(f"ClickHouse connection details: host={host}, port={port}, username={username}")
        
        return self.clickhouse_url
    
    def get_contributor_project_ids(self, contributor_id: str) -> List[str]:
        """Get project IDs for a contributor from PostgreSQL using comprehensive approach including distribution segments"""
        try:
            # Ensure PostgreSQL connection is established
            conn = self.get_postgres_connection()
            cursor = conn.cursor()
            project_ids = set()
            
            # PRIMARY METHOD: Use kepler-app's approach - findDistinctProjectIdsByContributorId
            # This is the exact query used in ContributorJobMappingRepo.findDistinctProjectIdsByContributorId
            primary_query = """
                SELECT DISTINCT project_id 
                FROM kepler_crowd_contributor_job_mapping_t 
                WHERE contributor_id = %s 
                AND project_id IS NOT NULL
            """
            
            logger.debug(f"🔍 PRIMARY METHOD - Using kepler-app's findDistinctProjectIdsByContributorId approach")
            logger.debug(f"   Query: {primary_query}")
            logger.debug(f"   Contributor ID: {contributor_id}")
            
            cursor.execute(primary_query, (contributor_id,))
            results = cursor.fetchall()
            primary_project_ids = [row[0] for row in results if row[0]]
            project_ids.update(primary_project_ids)
            logger.debug(f"   Found {len(primary_project_ids)} project IDs from job mapping: {primary_project_ids}")
            
            # SECONDARY METHOD: Also check project stats table (ContributorProjectStatsEntity)
            # This provides additional project associations that might not be in job mappings
            secondary_query = """
                SELECT DISTINCT project_id 
                FROM kepler_crowd_contributors_project_stats_t 
                WHERE contributor_id = %s 
                AND project_id IS NOT NULL
            """
            
            logger.debug(f"🔍 SECONDARY METHOD - Checking project stats table")
            logger.debug(f"   Query: {secondary_query}")
            
            cursor.execute(secondary_query, (contributor_id,))
            results2 = cursor.fetchall()
            secondary_project_ids = [row[0] for row in results2 if row[0]]
            project_ids.update(secondary_project_ids)
            logger.debug(f"   Found {len(secondary_project_ids)} additional project IDs from stats: {secondary_project_ids}")
            
            # TERTIARY METHOD: Check team mappings (if contributor is part of teams)
            tertiary_query = """
                SELECT DISTINCT project_id 
                FROM kepler_crowd_contributors_team_mapping_t 
                WHERE contributor_id = %s 
                AND project_id IS NOT NULL
            """
            
            logger.debug(f"🔍 TERTIARY METHOD - Checking team mappings")
            logger.debug(f"   Query: {tertiary_query}")
            
            tertiary_project_ids = []  # Initialize variable
            try:
                cursor.execute(tertiary_query, (contributor_id,))
                results3 = cursor.fetchall()
                tertiary_project_ids = [row[0] for row in results3 if row[0]]
                project_ids.update(tertiary_project_ids)
                logger.debug(f"   Found {len(tertiary_project_ids)} additional project IDs from teams: {tertiary_project_ids}")
            except Exception as team_error:
                logger.debug(f"   Team mapping query failed (table might not exist): {team_error}")
            
            # QUATERNARY METHOD: Check job relationships and distribution segment tables
            # These provide additional project associations through job mappings and actual work data
            logger.debug(f"🔍 QUATERNARY METHOD - Checking job relationships and distribution segments")
            job_relationship_project_ids = self._get_project_ids_from_job_relationships(cursor, contributor_id)
            project_ids.update(job_relationship_project_ids)
            logger.debug(f"   Found {len(job_relationship_project_ids)} additional project IDs from job relationships: {job_relationship_project_ids}")
            
            cursor.close()
            conn.close()  # Close the connection to avoid transaction issues
            
            final_project_ids = list(project_ids)
            logger.info(f"📊 Found {len(final_project_ids)} total project IDs for contributor {contributor_id}")
            logger.debug(f"   Final project IDs: {final_project_ids}")
            logger.debug(f"   Sources: Job mappings ({len(primary_project_ids)}), Stats ({len(secondary_project_ids)}), Teams ({len(tertiary_project_ids)}), Job relationships ({len(job_relationship_project_ids)})")
            
            return final_project_ids
            
        except Exception as e:
            logger.warning(f"⚠️  Error getting project IDs for contributor {contributor_id}: {e}")
            logger.debug(f"Exception details: {e}")
            # Ensure connection is closed even on error
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
            except:
                pass
            return []
    
    def _get_project_ids_from_job_relationships(self, cursor, contributor_id: str) -> List[str]:
        """Get project IDs from job-related tables (kepler-app's actual approach)"""
        try:
            project_ids = set()
            
            # METHOD 1: Direct contributor-job-project mapping via kepler_proj_job_contributor_t
            # This table links contributors to jobs, and jobs have project_ids
            logger.debug(f"   METHOD 1: Checking kepler_proj_job_contributor_t -> kepler_proj_job_t relationship")
            
            job_project_query = """
                SELECT DISTINCT pj.project_id
                FROM kepler_proj_job_contributor_t pjc
                JOIN kepler_proj_job_t pj ON pjc.job_id = pj.id
                WHERE pjc.contributor_id = %s
                AND pj.project_id IS NOT NULL
                AND pjc.status = 'ACTIVE'
            """
            
            cursor.execute(job_project_query, (contributor_id,))
            job_project_ids = [row[0] for row in cursor.fetchall() if row[0]]
            project_ids.update(job_project_ids)
            
            if job_project_ids:
                logger.debug(f"   Found {len(job_project_ids)} project IDs via job relationships: {job_project_ids}")
            
            # METHOD 2: Check distribution segment tables for additional project associations
            # These tables contain actual work distribution data
            logger.debug(f"   METHOD 2: Checking distribution segment tables")
            
            # Build distribution tables list dynamically using configurable shard count
            distribution_tables = [f'kepler_distribution_segment_t{i}' for i in range(DISTRIBUTION_SEGMENT_SHARD_COUNT)]
            
            for table_name in distribution_tables:
                try:
                    # Check if table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = %s
                        )
                    """, (table_name,))
                    
                    if not cursor.fetchone()[0]:
                        continue
                    
                    # Check if there are any records for this contributor
                    count_query = f"""
                        SELECT COUNT(*) FROM {table_name} 
                        WHERE (worker_id = %s OR last_annotator = %s)
                    """
                    cursor.execute(count_query, (contributor_id, contributor_id))
                    count = cursor.fetchone()[0]
                    
                    if count == 0:
                        continue
                    
                    # Get distinct project IDs for this contributor
                    project_query = f"""
                        SELECT DISTINCT project_id 
                        FROM {table_name} 
                        WHERE (worker_id = %s OR last_annotator = %s)
                        AND project_id IS NOT NULL
                    """
                    
                    cursor.execute(project_query, (contributor_id, contributor_id))
                    table_project_ids = [row[0] for row in cursor.fetchall() if row[0]]
                    project_ids.update(table_project_ids)
                    
                    if table_project_ids:
                        logger.debug(f"   Found project IDs in {table_name}: {table_project_ids}")
                
                except Exception as table_error:
                    logger.debug(f"   Error analyzing {table_name}: {table_error}")
                    continue
            
            return list(project_ids)
            
        except Exception as e:
            logger.debug(f"   Error getting project IDs from job relationships: {e}")
            return []
