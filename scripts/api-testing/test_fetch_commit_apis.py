#!/usr/bin/env python3
"""
Test Fetch/Commit APIs Script

This script tests the fetch and commit APIs for contributors to verify they can access the system.
Based on the pattern from gen_commit_1.py, this script tests:
- Fetch API: /dist/internal/fetch
- Commit API: /dist/internal/commit

This script is particularly useful for testing if deleted contributors can still access the system.
If a contributor has been properly deleted, they should receive a SYSTEM_ERROR response.

Based on kepler-app codebase analysis:
- Unit View data comes from project-* Elasticsearch indices
- Worker emails are stored in workerEmail arrays and various email fields
- PII masking should show "DELETED_USER" instead of original email addresses

Usage:
    python test_fetch_commit_apis.py --csv contributors.csv --job-url "https://client.appen.com/quality/jobs/..."
    python test_fetch_commit_apis.py --contributor-id CONTRIB_123 --job-url "https://client.appen.com/quality/jobs/..."
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from urllib.parse import urlparse, parse_qs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'test_fetch_commit_apis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
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

class FetchCommitAPITester:
    """Tests fetch and commit APIs for contributors"""
    
    def __init__(self, base_url: str = "http://localhost:9801"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Accept': '*/*',
            'User-Agent': 'ContributorDeletionTester/1.0'
        })
    
    def parse_job_url(self, job_url: str) -> Dict[str, str]:
        """Parse job URL to extract job ID and secret (based on gen_commit_1.py pattern)"""
        try:
            import re
            
            # Pattern to match: tasks/{jobId}?secret={secret}
            # Based on gen_commit_1.py parse_url function
            match = re.search(r'tasks/(?P<jobId>[\w-]+)\?secret=(?P<secret>[\w-]+)', job_url)
            if match:
                job_id = match.group('jobId')
                secret = match.group('secret')
                logger.info(f"Parsed jobId={job_id}, secret=<hidden>")
                
                result = {
                    'job_id': job_id,
                    'secret': secret,
                    'team_id': None  # Not used in this pattern
                }
                
                logger.info(f"Parsed job URL: {job_url}")
                logger.info(f"Extracted parameters: job_id={job_id}, secret=<hidden>")
                
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
                
                logger.info(f"Parsed job URL (fallback): {job_url}")
                logger.info(f"Extracted parameters: {result}")
                
                return result
            
        except Exception as e:
            logger.error(f"Error parsing job URL {job_url}: {e}")
            raise    
    def fetch_distributions(self, job_id: str, secret: str, worker_id: str) -> Dict:
        """
        Fetch distributions for a contributor (based on gen_commit_1.py pattern)
        
        Args:
            job_id: Job ID
            secret: Job secret
            worker_id: Contributor/Worker ID
            
        Returns:
            Dictionary with fetch results
        """
        logger.info(f"fetch_distributions ===== {job_id}, {secret}, {worker_id}")
        
        fetch_url = (
            f"{self.base_url}/dist/internal/fetch"
            f"?jobId={job_id}&workerId={worker_id}&pageNum=1&secret={secret}"
        )
        
        headers = {'Accept': '*/*'}
        
        logger.info(f"🚀 Executing Fetch API Request:")
        logger.info(f"   URL: {fetch_url}")
        logger.info(f"   Method: GET")
        logger.info(f"   Headers: {headers}")
        logger.info(f"   Worker ID: {worker_id}")
        logger.info(f"   Job ID: {job_id}")
        logger.info(f"   Page Number: 1")
        print(f"FETCH_URL={fetch_url}")
        
        start_time = time.time()
        try:
            response = requests.get(fetch_url, headers=headers, timeout=10)
            elapsed = time.time() - start_time
            
            # Log complete API response
            logger.info(f"📋 Complete Fetch API Response:")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Headers: {dict(response.headers)}")
            logger.info(f"   Response Body: {response.text}")
            logger.info(f"   Response Time: {elapsed:.2f}s")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"📋 Parsed Fetch Response Data:")
                logger.info(f"   {json.dumps(data, indent=2)}")
                # Print full JSON response to stdout
                print(json.dumps(data, indent=2))
                
                distributions = data.get('data', {}).get('distributions') or []
                dist_count = len(distributions)
                logger.info(f"✅ Fetched {dist_count} distributions in {elapsed:.2f}s")
                
                return {
                    'distributions': distributions,
                    'success': True,
                    'status_code': response.status_code,
                    'data': data,
                    'distributions_count': dist_count,
                    'error': None
                }
            else:
                error_msg = f"HTTP {response.status_code}: {response.text}"
                logger.error(f"❌ Fetch failed: {error_msg}")
                logger.error(f"📋 Complete Error Response:")
                logger.error(f"   Status Code: {response.status_code}")
                logger.error(f"   Response Headers: {dict(response.headers)}")
                logger.error(f"   Response Body: {response.text}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'data': None,
                    'distributions_count': 0,
                    'error': error_msg
                }
                
        except requests.exceptions.RequestException as e:
            logger.exception(f"❌ Failed to fetch distributions for worker {worker_id}: {e}")
            logger.error(f"📋 Exception Details:")
            logger.error(f"   Exception Type: {type(e).__name__}")
            logger.error(f"   Exception Message: {str(e)}")
            return {
                'success': False,
                'status_code': 0,
                'data': None,
                'distributions_count': 0,
                'error': str(e)
            }
    
    def create_commit_payload(self, distributions: List[Dict], option: str = "Yes") -> List[Dict]:
        """
        Create commit payload for distributions (based on gen_commit_1.py pattern)
        
        Args:
            distributions: List of distributions from fetch API
            option: Judgment option to commit
            
        Returns:
            List of distribution payloads for commit API
        """
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
        """
        Commit a single distribution (based on gen_commit_1.py pattern)
        
        Args:
            distribution: Distribution payload to commit
            
        Returns:
            Dictionary with commit results
        """
        commit_url = f"{self.base_url}/dist/internal/commit"
        headers = {'Accept': '*/*', 'Content-Type': 'application/json'}
        dist_id = distribution.get('id')

        logger.info(f"🚀 Executing Commit API Request:")
        logger.info(f"   URL: {commit_url}")
        logger.info(f"   Method: POST")
        logger.info(f"   Headers: {headers}")
        logger.info(f"   Distribution ID: {dist_id}")
        logger.info(f"📋 Complete Commit Request Payload:")
        logger.info(f"   {json.dumps([distribution], indent=2)}")
        print(f"COMMIT_URL={commit_url}  DISTRIBUTION_ID={dist_id}")
        
        start_time = time.time()
        try:
            response = self.session.post(commit_url, headers=headers, json=[distribution], timeout=10)
            elapsed = time.time() - start_time
            
            # Log complete API response
            logger.info(f"📋 Complete Commit API Response:")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Headers: {dict(response.headers)}")
            logger.info(f"   Response Body: {response.text}")
            logger.info(f"   Response Time: {elapsed:.2f}s")
            
            if response.status_code == 200:
                resp_data = response.json()
                logger.info(f"📋 Parsed Commit Response Data:")
                logger.info(f"   {json.dumps(resp_data, indent=2)}")
                # Print commit response JSON
                print(json.dumps(resp_data, indent=2))
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
                logger.error(f"📋 Complete Error Response:")
                logger.error(f"   Status Code: {response.status_code}")
                logger.error(f"   Response Headers: {dict(response.headers)}")
                logger.error(f"   Response Body: {response.text}")
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'data': None,
                    'error': error_msg
                }
                
        except Exception as e:
            logger.exception(f"❌ Error committing distribution {dist_id}: {e}")
            logger.error(f"📋 Exception Details:")
            logger.error(f"   Exception Type: {type(e).__name__}")
            logger.error(f"   Exception Message: {str(e)}")
            return {
                'success': False,
                'status_code': 0,
                'data': None,
                'error': str(e)
            }
    
    def test_contributor_access(self, contributor: ContributorInfo, job_url: str, commit_option: str = None) -> TestResult:
        """
        Test if a contributor can access the fetch/commit APIs
        
        Args:
            contributor: Contributor information
            job_url: Job URL to test access
            
        Returns:
            TestResult with test outcomes
        """
        logger.info(f"Testing access for contributor: {contributor.contributor_id} ({contributor.email_address})")
        
        # Parse job URL
        job_params = self.parse_job_url(job_url)
        job_id = job_params['job_id']
        secret = job_params['secret']
        
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
            filename = f"fetch_commit_test_results_{timestamp}.csv"
        
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
        logger.info("FETCH/COMMIT API TEST SUMMARY")
        logger.info("=" * 60)
        logger.info(f"📊 Total contributors tested: {total_tests}")
        logger.info(f"✅ Fetch API successes: {fetch_successes}/{total_tests} ({fetch_successes/total_tests*100:.1f}%)")
        logger.info(f"✅ Commit API successes: {commit_successes}/{total_tests} ({commit_successes/total_tests*100:.1f}%)")
        logger.info(f"📁 Total distributions fetched: {total_distributions}")
        logger.info("")
        
        # Response code breakdown
        fetch_codes = {}
        commit_codes = {}
        for result in results:
            fetch_codes[result.fetch_response_code] = fetch_codes.get(result.fetch_response_code, 0) + 1
            if result.commit_response_code > 0:
                commit_codes[result.commit_response_code] = commit_codes.get(result.commit_response_code, 0) + 1
        
        logger.info("📋 Fetch API response codes:")
        for code, count in sorted(fetch_codes.items()):
            logger.info(f"   {code}: {count}")
        
        if commit_codes:
            logger.info("📋 Commit API response codes:")
            for code, count in sorted(commit_codes.items()):
                logger.info(f"   {code}: {count}")
        
        logger.info("")
        logger.info("❌ Failed tests:")
        failed_count = 0
        for result in results:
            if not result.fetch_success:
                failed_count += 1
                logger.info(f"   {result.contributor_id} ({result.email_address}): {result.fetch_error}")
        
        if failed_count == 0:
            logger.info("   No failed tests!")
        
        logger.info("=" * 60)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Test fetch/commit APIs for contributors')
    parser.add_argument('--csv', help='CSV file containing contributor data')
    parser.add_argument('--contributor-id', help='Single contributor ID to test')
    parser.add_argument('--email', help='Email address for single contributor test')
    parser.add_argument('--job-url', required=True, help='Task URL with job ID and secret (e.g., https://account.integration.cf3.us/quality/tasks/JOB_ID?secret=SECRET)')
    parser.add_argument('--base-url', default='http://localhost:9801', help='Base URL for APIs (default: http://localhost:9801)')
    parser.add_argument('--output', help='Output CSV filename for test results')
    parser.add_argument('--sample-size', type=int, help='Number of contributors to test (default: test all)')
    parser.add_argument('--commit-option', help='Judgment option to commit (e.g., "Yes", "No"). If provided, will test commit API')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be tested without executing')
    
    args = parser.parse_args()
    
    # Validate input
    if not args.csv and not args.contributor_id:
        logger.error("Either --csv or --contributor-id must be specified")
        sys.exit(1)
    
    if args.csv and not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)
    
    try:
        # Initialize tester
        tester = FetchCommitAPITester(args.base_url)
        
        # Load contributors
        contributors = []
        if args.csv:
            contributors = tester.load_contributors_from_csv(args.csv)
        elif args.contributor_id:
            contributor = ContributorInfo(
                contributor_id=args.contributor_id,
                email_address=args.email or f"{args.contributor_id}@example.com"
            )
            contributors = [contributor]
        
        if not contributors:
            logger.error("No contributors to test")
            sys.exit(1)
        
        logger.info(f"Testing {len(contributors)} contributors with job URL: {args.job_url}")
        
        if args.dry_run:
            logger.info("DRY RUN: Would test the following contributors:")
            for contributor in contributors:
                logger.info(f"  - {contributor.contributor_id} ({contributor.email_address})")
            return
        
        # Test contributors
        results = []
        for i, contributor in enumerate(contributors, 1):
            logger.info(f"Testing contributor {i}/{len(contributors)}: {contributor.contributor_id}")
            result = tester.test_contributor_access(contributor, args.job_url, args.commit_option)
            results.append(result)
            
            # Small delay between tests
            time.sleep(0.5)
        
        # Print summary
        tester.print_test_summary(results)
        
        # Save results
        if results:
            csv_file = tester.save_test_results(results, args.output)
            logger.info(f"Test results saved to: {csv_file}")
        
        logger.info("API testing completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
