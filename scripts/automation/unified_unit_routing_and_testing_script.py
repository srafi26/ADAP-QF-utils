#!/usr/bin/env python3
"""
Unified Unit Routing and API Testing Script

This script combines unit routing workflow and fetch/commit API testing functionality:
1. Route units from a project to a specific job for testing
2. Test fetch/commit APIs for contributors to verify system access
3. Support both single contributor testing and batch processing
4. Dynamic configuration with environment variables

This unified script replaces both:
- unit_routing_workflow_script.py (unit routing functionality)
- test_fetch_commit_apis.py (API testing functionality)

Environment Variables (optional, with fallback defaults):
    DB_HOST, DATABASE_HOST - Database host
    DB_PORT, DATABASE_PORT - Database port (default: 5432)
    DB_NAME, DATABASE_NAME - Database name (default: kepler-app-db)
    DB_USER, DATABASE_USER - Database user (default: kepler-app-db-id)
    DB_PASSWORD, DATABASE_PASSWORD - Database password
    API_BASE_URL - API base URL (default: https://api-beta.integration.cf3.us)
    API_TIMEOUT - API timeout in seconds (default: 30)
    API_RETRY_COUNT - API retry count (default: 3)
    API_RATE_LIMIT - API rate limit (default: 100)
    WORKER_ID - Default worker ID
    API_TOKEN - Default API token
    JOB_SECRET - Default job secret

Usage Examples:

    # Unit Routing Mode (route units to job)
    python unified_unit_routing_and_testing_script.py \
      --mode routing \
      --project-id "e9f9661e-5e5e-43ef-ac1a-24b364405ea3" \
      --job-url "https://account.integration.cf3.us/quality/tasks/342da1e3-f879-4c36-9f9e-a59e9b1714c6?secret=YW5YsOGg992t8haOAXJQJASa3u97pIW9KIaGUXACvMaD2X" \
      --worker-id "worker-001" \
      --single-unit

    # API Testing Mode (test contributor access)
    python unified_unit_routing_and_testing_script.py \
      --mode testing \
      --csv "contributors.csv" \
      --job-url "https://account.integration.cf3.us/quality/tasks/342da1e3-f879-4c36-9f9e-a59e9b1714c6?secret=YW5YsOGg992t8haOAXJQJASa3u97pIW9KIaGUXACvMaD2X" \
      --sample-size 5

    # Combined Mode (route units then test APIs)
    python unified_unit_routing_and_testing_script.py \
      --mode combined \
      --project-id "e9f9661e-5e5e-43ef-ac1a-24b364405ea3" \
      --csv "contributors.csv" \
      --job-url "https://account.integration.cf3.us/quality/tasks/342da1e3-f879-4c36-9f9e-a59e9b1714c6?secret=YW5YsOGg992t8haOAXJQJASa3u97pIW9KIaGUXACvMaD2X" \
      --single-unit
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import requests
import psycopg2
import re
import random
import configparser
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('unified_unit_routing_and_testing.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ContributorInfo:
    """Data class for contributor information"""
    contributor_id: str
    email_address: str
    name: Optional[str] = None

@dataclass
class TestResult:
    """Data class for test results"""
    contributor_id: str
    email_address: str
    job_id: str
    fetch_success: bool
    fetch_response_code: int
    fetch_error: Optional[str] = None
    commit_success: bool = False
    commit_response_code: int = 0
    commit_error: Optional[str] = None
    distributions_count: int = 0

class UnifiedUnitRoutingAndTesting:
    """Unified class for unit routing and API testing functionality"""
    
    def __init__(self, 
                 project_id: str = None,
                 job_id: str = None,
                 worker_id: str = None,
                 api_token: str = None,
                 secret: str = None,
                 judgment_option: str = "first_option",
                 base_url: str = None,
                 db_config: Optional[Dict] = None,
                 api_config: Optional[Dict] = None):
        
        self.project_id = project_id
        self.job_id = job_id
        self.worker_id = worker_id or os.getenv('WORKER_ID', 'd8369b01-d816-4f90-8593-ca83208edb6d')
        self.api_token = api_token or os.getenv('API_TOKEN', 'CgjVt3SvJfoQn_buVuaM')
        self.secret = secret or os.getenv('JOB_SECRET', 'VCGGPn3gBcFnFYEPFI4uhaMCxGj7C2PNhGpFxxRbvup6sK')
        self.judgment_option = judgment_option
        self.base_url = (base_url or os.getenv('API_BASE_URL', 'https://api-beta.integration.cf3.us')).rstrip('/')
        
        # Database configuration (dynamic based on environment)
        self.db_config = db_config or self._get_default_db_config()
        
        # API configuration
        self.api_config = api_config or {
            'timeout': int(os.getenv('API_TIMEOUT', '30')),
            'retry_count': int(os.getenv('API_RETRY_COUNT', '3')),
            'rate_limit': os.getenv('API_RATE_LIMIT', '100')
        }
        
        # Initialize statistics
        self.stats = {
            'units_found': 0,
            'units_routed': 0,
            'assignments_fetched': 0,
            'assignments_committed': 0,
            'failed_assignments': 0,
            'loop_iterations': 0,
            'contributors_tested': 0,
            'fetch_successes': 0,
            'commit_successes': 0
        }
        
        # Unit tracking file
        if self.project_id:
            self.tracking_file = f"processed_units_{self.project_id}.json"
            self.processed_units = self.load_processed_units()
        else:
            self.tracking_file = None
            self.processed_units = set()
        
        # Initialize session and headers
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Token token={self.api_token}',
            'Content-Type': 'application/json'
        })
        
        # Distribution service headers (for localhost endpoints)
        self.dist_headers = {
            'Content-Type': 'application/json'
        }
        
        # Initialize database connection
        self.db_connection = None
    
    def _get_default_db_config(self) -> Dict:
        """Get default database configuration from environment variables or sensible defaults"""
        # Force integration environment for safety
        default_host = 'kepler-pg-integration.cluster-ce52lgdtaew6.us-east-1.rds.amazonaws.com'
        default_password = '6g3evdSsVVErzGT0ALp7gGwYiccwmZSb'
        
        # Check if environment variables point to production and warn
        env_host = os.getenv('DB_HOST', os.getenv('DATABASE_HOST', ''))
        if 'live' in env_host or 'production' in env_host:
            logger.warning("⚠️ Environment variables point to PRODUCTION database! Using integration defaults for safety.")
            env_host = default_host
            env_password = default_password
        else:
            env_host = env_host or default_host
            env_password = os.getenv('DB_PASSWORD', os.getenv('DATABASE_PASSWORD', default_password))
        
        return {
            'host': env_host,
            'port': int(os.getenv('DB_PORT', os.getenv('DATABASE_PORT', '5432'))),
            'database': os.getenv('DB_NAME', os.getenv('DATABASE_NAME', 'kepler-app-db')),
            'user': os.getenv('DB_USER', os.getenv('DATABASE_USER', 'kepler-app-db-id')),
            'password': env_password
        }
    
    
    def parse_job_url(self, job_url: str) -> Dict[str, str]:
        """Parse job URL to extract job ID and secret"""
        try:
            # Pattern to match: /quality/tasks/{job_id}?secret={secret}
            pattern = r'/quality/tasks/([a-f0-9-]+)\?secret=([a-zA-Z0-9]+)'
            match = re.search(pattern, job_url)
            
            if match:
                job_id = match.group(1)
                secret = match.group(2)
                logger.info(f"Parsed job_id: {job_id}")
                logger.info(f"Parsed secret: {secret[:8]}...{secret[-8:]}")
                
                result = {
                    'job_id': job_id,
                    'secret': secret,
                    'team_id': None
                }
                
                return result
            else:
                # Fallback: try to parse as regular URL
                parsed_url = urlparse(job_url)
                path_parts = parsed_url.path.strip('/').split('/')
                
                # Extract job ID from URL path
                job_id = None
                if 'jobs' in path_parts:
                    job_index = path_parts.index('jobs')
                    if job_index + 1 < len(path_parts):
                        job_id = path_parts[job_index + 1]
                
                # Extract query parameters
                query_params = parse_qs(parsed_url.query)
                
                result = {
                    'job_id': job_id,
                    'team_id': query_params.get('teamId', [None])[0],
                    'secret': query_params.get('secret', [None])[0]
                }
                
                return result
            
        except Exception as e:
            logger.error(f"Error parsing job URL {job_url}: {e}")
            raise
    
    # ============================================================================
    # UNIT ROUTING FUNCTIONALITY
    # ============================================================================
    
    def load_processed_units(self) -> set:
        """Load previously processed unit IDs from tracking file"""
        if not self.tracking_file:
            return set()
            
        try:
            if os.path.exists(self.tracking_file):
                with open(self.tracking_file, 'r') as f:
                    data = json.load(f)
                    processed = set(data.get('processed_units', []))
                    logger.info(f"📋 Loaded {len(processed)} previously processed units from {self.tracking_file}")
                    return processed
            else:
                logger.info(f"📋 No tracking file found, starting fresh")
                return set()
        except Exception as e:
            logger.warning(f"⚠️ Failed to load tracking file: {e}, starting fresh")
            return set()
    
    def save_processed_units(self, new_units: List[str]):
        """Save newly processed unit IDs to tracking file"""
        if not self.tracking_file:
            return
            
        try:
            # Add new units to processed set
            self.processed_units.update(new_units)
            
            # Save to file
            data = {
                'project_id': self.project_id,
                'last_updated': datetime.now().isoformat(),
                'total_processed': len(self.processed_units),
                'processed_units': list(self.processed_units)
            }
            
            with open(self.tracking_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"💾 Saved {len(new_units)} new units to tracking file. Total processed: {len(self.processed_units)}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save tracking file: {e}")
    
    def connect_database(self) -> bool:
        """Connect to PostgreSQL database"""
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            logger.info("✅ Database connection established")
            logger.info(f"🔍 Connected to database: {self.db_config.get('host')}:{self.db_config.get('port')}/{self.db_config.get('database')}")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    
    def query_sharded_unit_segments(self, limit: int = 70) -> List[Dict]:
        """Query kepler_proj_unit_segment_t sharded tables (t0..t9) for project_id"""
        logger.info(f"🔍 Querying sharded unit segments for project_id: {self.project_id}")
        
        units = []
        shard_count = 10  # t0 through t9
        
        try:
            cursor = self.db_connection.cursor()
            
            for shard in range(shard_count):
                table_name = f"kepler_proj_unit_segment_t{shard}"
                
                query = f"""
                SELECT id, project_id, display_id, segment_group_id, content, status, 
                       source_file_batch_number, row_number, unit_segment_type
                FROM {table_name}
                WHERE project_id = %s 
                AND status = 'ACTIVE'
                ORDER BY created_at ASC
                LIMIT %s
                """
                
                logger.info(f"  Executing query on {table_name} for project_id: {self.project_id}")
                cursor.execute(query, (self.project_id, limit))
                shard_units = cursor.fetchall()
                logger.info(f"  Shard {shard}: Found {len(shard_units)} units")
                
                for unit in shard_units:
                    units.append({
                        'id': unit[0],
                        'project_id': unit[1],
                        'display_id': unit[2],
                        'segment_group_id': unit[3],
                        'content': unit[4],
                        'status': unit[5],
                        'source_file_batch_number': unit[6],
                        'row_number': unit[7],
                        'unit_segment_type': unit[8],
                        'shard': shard
                    })
                
                logger.info(f"  Shard {shard}: Found {len(shard_units)} units")
            
            cursor.close()
            self.stats['units_found'] = len(units)
            logger.info(f"✅ Total units found: {len(units)}")
            return units
            
        except Exception as e:
            logger.error(f"❌ Database query failed: {e}")
            return []
    
    def route_units_to_job(self, unit_ids: List[str]) -> bool:
        """Route units to job using send-to-job API"""
        logger.info(f"🚀 Routing {len(unit_ids)} units to job: {self.job_id}")
        
        url = f"{self.base_url}/v3/project-flow/projects/send-to-job"
        params = {
            'jobId': self.job_id,
            'ignoreConflict': 'false'
        }
        
        payload = {
            "projectId": self.project_id,
            "unitIds": unit_ids,
            "percentage": 100,
            "sendEntireGroup": False,
            "carryJudgment": True,
            "overwriteJudgment": True
        }
        
        # Use specific API key for send-to-job endpoint
        api_key = self.config.get('api', 'send_to_job_api_key')
        headers = {
            'Authorization': f'Token token={api_key}',
            'Content-Type': 'application/json'
        }
        
        logger.info(f"🔑 Using API key: {api_key[:8]}... for send-to-job endpoint")
        
        try:
            response = requests.post(url, params=params, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Units routed successfully: {data.get('message', 'success')}")
                self.stats['units_routed'] += len(unit_ids)
                return True
            else:
                logger.error(f"❌ Route units failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Route units error: {e}")
            return False
    
    def run_unit_routing_workflow(self, batch_size: int = 10, max_units: int = 70) -> bool:
        """Run the unit routing workflow"""
        logger.info("🚀 Starting Unit Routing Workflow")
        logger.info(f"Project ID: {self.project_id}")
        logger.info(f"Job ID: {self.job_id}")
        logger.info(f"Worker ID: {self.worker_id}")
        logger.info(f"Batch Size: {batch_size}")
        logger.info(f"Max Units: {max_units}")
        
        # Connect to database
        if not self.connect_database():
            return False
        
        # Query unit segments from sharded tables
        all_units = self.query_sharded_unit_segments(limit=max_units)
        if not all_units:
            logger.error("❌ No units found")
            return False
        
        # Filter out already processed units
        unprocessed_units = [unit for unit in all_units if unit['id'] not in self.processed_units]
        if not unprocessed_units:
            logger.warning("⚠️ No unprocessed units found")
            return True
        
        logger.info(f"🔄 Processing {len(unprocessed_units)} unprocessed units out of {len(all_units)} total units")
        
        # Process units in batches
        total_processed = 0
        for i in range(0, len(unprocessed_units), batch_size):
            batch = unprocessed_units[i:i + batch_size]
            self.stats['loop_iterations'] += 1
            
            logger.info(f"\n🔄 Processing batch {i//batch_size + 1}")
            
            # Route units to job
            unit_ids = [unit['id'] for unit in batch]
            if self.route_units_to_job(unit_ids):
                total_processed += len(batch)
                self.save_processed_units(unit_ids)
                logger.info(f"✅ Batch completed successfully. Total processed: {total_processed}")
            else:
                logger.error(f"❌ Batch failed")
                break
            
            # Small delay between batches
            time.sleep(2)
        
        logger.info(f"✅ Unit routing workflow completed. Total processed: {total_processed}")
        return True
    
    # ============================================================================
    # API TESTING FUNCTIONALITY
    # ============================================================================
    
    def fetch_distributions(self, job_id: str, secret: str, worker_id: str) -> Dict:
        """Fetch distributions for a contributor"""
        logger.info(f"fetch_distributions ===== {job_id}, {secret}, {worker_id}")
        
        fetch_url = (
            f"http://localhost:9801/dist/internal/fetch"
            f"?jobId={job_id}&workerId={worker_id}&pageNum=1&secret={secret}"
        )
        
        headers = {'Accept': '*/*'}
        
        logger.info(f"🚀 Executing Fetch API Request:")
        logger.info(f"   URL: {fetch_url}")
        logger.info(f"   Worker ID: {worker_id}")
        
        start_time = time.time()
        try:
            response = requests.get(fetch_url, headers=headers, timeout=10)
            elapsed = time.time() - start_time
            
            # 📊 COMPREHENSIVE API RESPONSE LOGGING
            logger.info("=" * 80)
            logger.info("📊 FULL API RESPONSE DETAILS")
            logger.info("=" * 80)
            logger.info(f"🔗 Request URL: {fetch_url}")
            logger.info(f"📤 Request Headers: {dict(headers)}")
            logger.info(f"⏱️  Response Time: {elapsed:.3f}s")
            logger.info(f"📊 Status Code: {response.status_code}")
            logger.info(f"📋 Response Headers: {dict(response.headers)}")
            logger.info(f"📄 Response Text (first 500 chars): {response.text[:500]}")
            if len(response.text) > 500:
                logger.info(f"📄 Response Text (remaining): ... ({len(response.text) - 500} more chars)")
            logger.info("=" * 80)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    
                    if data is None:
                        logger.warning("⚠️ Response data is None, using empty dict")
                        data = {}
                    
                    # Handle case where data.data is None (SYSTEM_ERROR response)
                    data_section = data.get('data')
                    if data_section is None:
                        if data.get('message') == 'SYSTEM_ERROR':
                            logger.error(f"❌ System error from API: {data.get('message')} (code: {data.get('code')})")
                            return {
                                'success': False,
                                'status_code': response.status_code,
                                'data': data,
                                'distributions_count': 0,
                                'error': f"System error: {data.get('message')}"
                            }
                        data_section = {}
                    
                    distributions = data_section.get('distributions') or []
                    dist_count = len(distributions)
                    logger.info(f"✅ Fetched {dist_count} distributions in {elapsed:.2f}s")
                except (ValueError, TypeError) as e:
                    logger.error(f"❌ Failed to parse JSON response: {e}")
                    logger.error(f"Response text: {response.text}")
                    return {
                        'success': False,
                        'status_code': response.status_code,
                        'data': None,
                        'distributions_count': 0,
                        'error': f"JSON parsing error: {e}"
                    }
                
                return {
                    'distributions': distributions,
                    'success': True,
                    'status_code': response.status_code,
                    'data': data,
                    'distributions_count': dist_count,
                    'error': None
                }
            else:
                # 📊 NON-200 STATUS CODE LOGGING
                logger.error("=" * 80)
                logger.error("❌ NON-200 STATUS CODE RESPONSE")
                logger.error("=" * 80)
                logger.error(f"📊 Status Code: {response.status_code}")
                logger.error(f"📋 Response Headers: {dict(response.headers)}")
                logger.error(f"📄 Full Response Text: {response.text}")
                logger.error("=" * 80)
                
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"❌ Fetch failed: {error_msg}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'data': None,
                    'distributions_count': 0,
                    'error': error_msg
                }
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to fetch distributions for worker {worker_id}: {e}")
            return {
                'success': False,
                'status_code': 0,
                'data': None,
                'distributions_count': 0,
                'error': str(e)
            }
    
    def create_commit_payload(self, distributions: List[Dict], option: str = "Yes") -> List[Dict]:
        """Create commit payload for distributions"""
        payload = []
        for dist in distributions:
            segments = []
            for seg in dist.get('segments', []):
                segments.append({
                    'id': seg.get('id'),
                    'judgment': {
                        'ask_question_here': option,
                        '_meta': {
                            'ask_question_here': {
                                'validators': ['required'],
                                'fieldType': 'Radios'
                            }
                        }
                    },
                    'asArrayForSpecificApi': False
                })
            payload.append({
                'id': dist.get('id'),
                'workerId': dist.get('workerId'),
                'shardingKey': dist.get('shardingKey'),
                'segments': segments
            })
        logger.info(f"Created commit payload with {len(payload)} distributions (option={option})")
        return payload

    def commit_distribution(self, distribution: Dict) -> Dict:
        """Commit a single distribution"""
        commit_url = f"http://localhost:9801/dist/internal/commit"
        headers = {'Accept': '*/*', 'Content-Type': 'application/json'}
        dist_id = distribution.get('id')

        logger.info(f"🚀 Executing Commit API Request:")
        logger.info(f"   Distribution ID: {dist_id}")
        
        start_time = time.time()
        try:
            response = self.session.post(commit_url, headers=headers, json=[distribution], timeout=10)
            elapsed = time.time() - start_time
            
            if response.status_code == 200:
                resp_data = response.json()
                logger.info(f"✅ Committed {dist_id} in {elapsed:.2f}s")
                
                return {
                    'success': True,
                    'status_code': response.status_code,
                    'data': resp_data,
                    'error': None
                }
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"❌ Commit failed for {dist_id}: {error_msg}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'data': None,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.error(f"❌ Error committing distribution {dist_id}: {e}")
            return {
                'success': False,
                'status_code': 0,
                'data': None,
                'error': str(e)
            }
    
    def test_contributor_access(self, contributor: ContributorInfo, job_url: str, commit_option: str = None) -> TestResult:
        """Test if a contributor can access the fetch/commit APIs"""
        logger.info(f"Testing access for contributor: {contributor.contributor_id} ({contributor.email_address})")
        
        # Parse job URL
        job_params = self.parse_job_url(job_url)
        job_id = job_params.get('job_id')
        secret = job_params.get('secret')
        
        if not job_id or not secret:
            logger.error(f"Could not extract job_id or secret from URL: {job_url}")
            return TestResult(
                contributor_id=contributor.contributor_id,
                email_address=contributor.email_address,
                job_id=job_id or 'UNKNOWN',
                fetch_success=False,
                fetch_response_code=0,
                fetch_error="Could not parse job URL"
            )
        
        # Test fetch API
        fetch_result = self.fetch_distributions(job_id, secret, contributor.contributor_id)
        
        test_result = TestResult(
            contributor_id=contributor.contributor_id,
            email_address=contributor.email_address,
            job_id=job_id,
            fetch_success=fetch_result['success'],
            fetch_response_code=fetch_result['status_code'],
            fetch_error=fetch_result['error'],
            distributions_count=fetch_result['distributions_count']
        )
        
        # Test commit API if option provided and fetch was successful
        if commit_option and fetch_result['success'] and fetch_result['distributions']:
            logger.info(f"Testing commit API with option: {commit_option}")
            
            # Create commit payload
            commit_payload = self.create_commit_payload(fetch_result['distributions'], commit_option)
            
            # Commit each distribution
            commit_successes = 0
            commit_errors = []
            
            for distribution in commit_payload:
                commit_result = self.commit_distribution(distribution)
                if commit_result['success']:
                    commit_successes += 1
                else:
                    commit_errors.append(commit_result['error'])
            
            # Update test result with commit information
            test_result.commit_success = commit_successes > 0
            test_result.commit_response_code = 200 if commit_successes > 0 else 400
            test_result.commit_error = '; '.join(commit_errors) if commit_errors else None
            
            logger.info(f"Commit results: {commit_successes}/{len(commit_payload)} successful")
        
        # Update statistics
        self.stats['contributors_tested'] += 1
        if test_result.fetch_success:
            self.stats['fetch_successes'] += 1
        if test_result.commit_success:
            self.stats['commit_successes'] += 1
        
        return test_result
    
    def load_contributors_from_csv(self, csv_file: str) -> List[ContributorInfo]:
        """Load contributors from CSV file"""
        contributors = []
        
        logger.info(f"Loading contributors from CSV: {csv_file}")
        
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
        
        logger.info(f"Loaded {len(contributors)} contributors from CSV")
        return contributors
    
    def save_test_results(self, results: List[TestResult], filename: str = None) -> str:
        """Save test results to CSV file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"unified_test_results_{timestamp}.csv"
        
        filepath = os.path.expanduser(f"~/contributor-deletion-system-package/backups/{filename}")
        
        # Ensure backup directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        logger.info(f"Saving {len(results)} test results to CSV: {filepath}")
        
        with open(filepath, 'w', newline='', encoding='utf-8') as file:
            fieldnames = [
                'contributor_id', 'email_address', 'job_id', 'fetch_success', 
                'fetch_response_code', 'fetch_error', 'commit_success', 
                'commit_response_code', 'commit_error', 'distributions_count'
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            
            writer.writeheader()
            for result in results:
                writer.writerow({
                    'contributor_id': result.contributor_id,
                    'email_address': result.email_address,
                    'job_id': result.job_id,
                    'fetch_success': result.fetch_success,
                    'fetch_response_code': result.fetch_response_code,
                    'fetch_error': result.fetch_error or '',
                    'commit_success': result.commit_success,
                    'commit_response_code': result.commit_response_code,
                    'commit_error': result.commit_error or '',
                    'distributions_count': result.distributions_count
                })
        
        logger.info(f"Test results saved to: {filepath}")
        return filepath
    
    def print_test_summary(self, results: List[TestResult]):
        """Print test summary statistics"""
        if not results:
            logger.info("No test results to summarize")
            return
        
        total_tests = len(results)
        fetch_successes = sum(1 for r in results if r.fetch_success)
        commit_successes = sum(1 for r in results if r.commit_success)
        total_distributions = sum(r.distributions_count for r in results)
        
        logger.info("=" * 60)
        logger.info("UNIFIED API TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📊 Total contributors tested: {total_tests}")
        logger.info(f"✅ Fetch API successes: {fetch_successes}/{total_tests} ({fetch_successes/total_tests*100:.1f}%)")
        logger.info(f"✅ Commit API successes: {commit_successes}/{total_tests} ({commit_successes/total_tests*100:.1f}%)")
        logger.info(f"📁 Total distributions fetched: {total_distributions}")
        logger.info("=" * 60)
    
    def run_api_testing_workflow(self, contributors: List[ContributorInfo], job_url: str, commit_option: str = None) -> List[TestResult]:
        """Run the API testing workflow"""
        logger.info("🚀 Starting API Testing Workflow")
        logger.info(f"Contributors to test: {len(contributors)}")
        logger.info(f"Job URL: {job_url}")
        if commit_option:
            logger.info(f"Commit option: {commit_option}")
        
        results = []
        for i, contributor in enumerate(contributors, 1):
            logger.info(f"Testing contributor {i}/{len(contributors)}: {contributor.contributor_id}")
            result = self.test_contributor_access(contributor, job_url, commit_option)
            results.append(result)
            
            # Small delay between tests
            time.sleep(0.5)
        
        # Print summary
        self.print_test_summary(results)
        
        return results
    
    # ============================================================================
    # COMBINED WORKFLOW
    # ============================================================================
    
    def run_combined_workflow(self, contributors: List[ContributorInfo], job_url: str, 
                            batch_size: int = 1, max_units: int = 1, commit_option: str = None) -> bool:
        """Run combined unit routing and API testing workflow"""
        logger.info("🚀 Starting Combined Unit Routing and API Testing Workflow")
        logger.info("=" * 80)
        
        # Step 1: Route units to job
        logger.info("STEP 1: ROUTING UNITS TO JOB")
        logger.info("-" * 40)
        routing_success = self.run_unit_routing_workflow(batch_size=batch_size, max_units=max_units)
        
        if not routing_success:
            logger.error("❌ Unit routing failed, skipping API testing")
            return False
        
        # Step 2: Wait a bit for units to be available
        logger.info("\nSTEP 2: WAITING FOR UNITS TO BE AVAILABLE")
        logger.info("-" * 40)
        time.sleep(5)
        logger.info("✅ Wait completed")
        
        # Step 3: Test API access for contributors
        logger.info("\nSTEP 3: TESTING API ACCESS FOR CONTRIBUTORS")
        logger.info("-" * 40)
        results = self.run_api_testing_workflow(contributors, job_url, commit_option)
        
        # Step 4: Save results
        if results:
            csv_file = self.save_test_results(results)
            logger.info(f"✅ Combined workflow completed. Results saved to: {csv_file}")
        
        return True

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Unified Unit Routing and API Testing Script')
    
    # Mode selection
    parser.add_argument('--mode', required=True, choices=['routing', 'testing', 'combined'],
                       help='Operation mode: routing (unit routing only), testing (API testing only), combined (both)')
    
    # Common arguments
    parser.add_argument('--job-url', required=True, help='Job URL to parse job_id and secret from')
    parser.add_argument('--config-file', default='~/config_integration.ini', 
                       help='Path to the configuration INI file')
    parser.add_argument('--environment', default='integration', choices=['integration'],
                       help='Environment to use for configuration (integration only for safety)')
    
    # Unit routing arguments
    parser.add_argument('--project-id', help='Project ID to process (required for routing/combined modes)')
    parser.add_argument('--worker-id', default=os.getenv('WORKER_ID', 'd8369b01-d816-4f90-8593-ca83208edb6d'), 
                       help='Worker ID for assignments')
    parser.add_argument('--api-token', default=os.getenv('API_TOKEN', 'CgjVt3SvJfoQn_buVuaM'), 
                       help='API token for authentication')
    parser.add_argument('--option', default='first_option', 
                       help='Judgment option (first_option, second_option, etc.)')
    parser.add_argument('--base-url', default=os.getenv('API_BASE_URL', 'https://api-beta.integration.cf3.us'), 
                       help='Base URL for API calls')
    parser.add_argument('--batch-size', type=int, default=10, 
                       help='Number of units to process per batch')
    parser.add_argument('--max-units', type=int, default=70, 
                       help='Maximum number of units to process')
    parser.add_argument('--single-unit', action='store_true',
                       help='Process only one unit at a time (overrides batch-size)')
    
    # API testing arguments
    parser.add_argument('--csv', help='CSV file containing contributor data (required for testing/combined modes)')
    parser.add_argument('--contributor-id', help='Single contributor ID to test')
    parser.add_argument('--email', help='Email address for single contributor test')
    parser.add_argument('--sample-size', type=int, help='Number of contributors to test (default: test all)')
    parser.add_argument('--commit-option', help='Judgment option to commit (e.g., "Yes", "No"). If provided, will test commit API')
    parser.add_argument('--output', help='Output CSV filename for test results')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be tested without executing')
    
    args = parser.parse_args()
    
    # Validate arguments based on mode
    if args.mode in ['routing', 'combined'] and not args.project_id:
        logger.error("--project-id is required for routing and combined modes")
        sys.exit(1)
    
    if args.mode in ['testing', 'combined'] and not args.csv and not args.contributor_id:
        logger.error("Either --csv or --contributor-id is required for testing and combined modes")
        sys.exit(1)
    
    if args.csv and not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)
    
    try:
        # Parse job URL
        job_params = {}
        if args.job_url:
            # Create a temporary instance to parse the job URL
            temp_instance = UnifiedUnitRoutingAndTesting()
            try:
                job_params = temp_instance.parse_job_url(args.job_url)
            except Exception as e:
                logger.error(f"Failed to parse job URL: {e}")
                return 1
        
        # Load configuration file (default to integration for safety)
        config = {}
        config_file_path = os.path.expanduser(args.config_file)
        if os.path.exists(config_file_path):
            import configparser
            config_parser = configparser.ConfigParser()
            config_parser.read(config_file_path)
            for section in config_parser.sections():
                config[section] = dict(config_parser[section])
            logger.info(f"📁 Loaded configuration from: {config_file_path}")
        else:
            logger.warning(f"⚠️ Configuration file not found: {config_file_path}, using defaults")
        
        # Extract configurations with safety checks
        default_host = 'kepler-pg-integration.cluster-ce52lgdtaew6.us-east-1.rds.amazonaws.com'
        default_password = '6g3evdSsVVErzGT0ALp7gGwYiccwmZSb'
        
        # Check if environment variables point to production and warn
        env_host = os.getenv('DB_HOST', os.getenv('DATABASE_HOST', ''))
        if 'live' in env_host or 'production' in env_host:
            logger.warning("⚠️ Environment variables point to PRODUCTION database! Using integration defaults for safety.")
            env_host = default_host
            env_password = default_password
        else:
            env_host = env_host or default_host
            env_password = os.getenv('DB_PASSWORD', os.getenv('DATABASE_PASSWORD', default_password))
        
        db_config = {
            'host': env_host,
            'port': int(os.getenv('DB_PORT', os.getenv('DATABASE_PORT', '5432'))),
            'database': os.getenv('DB_NAME', os.getenv('DATABASE_NAME', 'kepler-app-db')),
            'user': os.getenv('DB_USER', os.getenv('DATABASE_USER', 'kepler-app-db-id')),
            'password': env_password
        }
        
        
        api_config = {
            'timeout': int(os.getenv('API_TIMEOUT', '30')),
            'retry_count': int(os.getenv('API_RETRY_COUNT', '3')),
            'rate_limit': os.getenv('API_RATE_LIMIT', '100')
        }
        
        # Create unified instance
        unified = UnifiedUnitRoutingAndTesting(
            project_id=args.project_id,
            job_id=job_params.get('job_id') if job_params else None,
            worker_id=args.worker_id,
            api_token=args.api_token,
            secret=job_params.get('secret') if job_params else None,
            judgment_option=args.option,
            base_url=args.base_url,
            db_config=db_config,
            api_config=api_config
        )
        
        # Load contributors if needed
        contributors = []
        if args.mode in ['testing', 'combined']:
            if args.csv:
                contributors = unified.load_contributors_from_csv(args.csv)
            elif args.contributor_id:
                contributor = ContributorInfo(
                    contributor_id=args.contributor_id,
                    email_address=args.email or f"{args.contributor_id}@example.com"
                )
                contributors = [contributor]
            
            # Apply sample size if specified
            if args.sample_size and len(contributors) > args.sample_size:
                contributors = contributors[:args.sample_size]
                logger.info(f"Limited to {args.sample_size} contributors for testing")
        
        if args.dry_run:
            logger.info("DRY RUN: Would execute the following:")
            if args.mode in ['routing', 'combined']:
                logger.info(f"  - Unit routing for project {args.project_id}")
            if args.mode in ['testing', 'combined']:
                logger.info(f"  - API testing for {len(contributors)} contributors")
            return
        
        # Execute based on mode
        success = False
        
        if args.mode == 'routing':
            # Unit routing only
            batch_size = 1 if args.single_unit else args.batch_size
            max_units = 1 if args.single_unit else args.max_units
            success = unified.run_unit_routing_workflow(batch_size=batch_size, max_units=max_units)
            
        elif args.mode == 'testing':
            # API testing only
            results = unified.run_api_testing_workflow(contributors, args.job_url, args.option)
            if results:
                csv_file = unified.save_test_results(results, args.output)
                logger.info(f"Test results saved to: {csv_file}")
            success = True
            
        elif args.mode == 'combined':
            # Combined workflow
            batch_size = 1 if args.single_unit else args.batch_size
            max_units = 1 if args.single_unit else args.max_units
            success = unified.run_combined_workflow(contributors, args.job_url, 
                                                  batch_size=batch_size, max_units=max_units, 
                                                  commit_option=args.option)
        
        if success:
            logger.info("✅ Unified workflow completed successfully")
            return 0
        else:
            logger.error("❌ Unified workflow failed")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Script failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
