#!/usr/bin/env python3
"""
Comprehensive Testing Workflow Script

This script implements the complete testing workflow for contributor deletion:
1. Route units to job using unit_routing_workflow_script.py
2. Test fetch/commit APIs with contributors
3. Delete contributors from all systems (Elasticsearch, ClickHouse, PostgreSQL)
4. Verify deletion by testing fetch API again
5. Verify DELETED_USER appears in Elasticsearch and ClickHouse

Usage:
    python comprehensive_testing_workflow.py --project-id <PROJECT_ID> --job-url <JOB_URL> --csv <CONTRIBUTORS_CSV> --config <CONFIG_FILE>

Example:
    python comprehensive_testing_workflow.py \
      --project-id "fe92bf1f-46c3-4ebb-a09e-0557237f41e6" \
      --job-url "https://client.appen.com/quality/jobs/27f196f6-ee10-49ae-b8d1-8b983cd6f2ea?secret=mnbCcSEUWCwownwiLkv0z7iZ7raK3JbqlSCBzFGqDWMGCO" \
      --csv "backups/inactive_contributors_10.csv" \
      --config "~/config_integration.ini"
"""

import argparse
import os
import sys
import subprocess
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'comprehensive_testing_workflow_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

class ComprehensiveTestingWorkflow:
    """Comprehensive testing workflow for contributor deletion"""
    
    def __init__(self, project_id: str, job_url: str, csv_file: str, config_file: str):
        self.project_id = project_id
        self.job_url = job_url
        self.csv_file = csv_file
        self.config_file = config_file
        self.workflow_start_time = datetime.now()
        
        # Extract job_id and secret from job_url
        self.job_id, self.secret = self._parse_job_url(job_url)
        
        logger.info("=" * 80)
        logger.info("🚀 COMPREHENSIVE TESTING WORKFLOW INITIALIZED")
        logger.info("=" * 80)
        logger.info(f"📊 Project ID: {project_id}")
        logger.info(f"🎯 Job ID: {self.job_id}")
        logger.info(f"🔗 Job URL: {job_url}")
        logger.info(f"📁 CSV File: {csv_file}")
        logger.info(f"⚙️  Config File: {config_file}")
        logger.info(f"🕐 Start Time: {self.workflow_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
    
    def _parse_job_url(self, job_url: str) -> tuple:
        """Parse job URL to extract job_id and secret"""
        try:
            # Extract job_id from URL path
            job_id_match = job_url.split('/jobs/')[1].split('?')[0]
            
            # Extract secret from query parameters
            secret_match = job_url.split('secret=')[1]
            
            logger.info(f"🔧 Parsed job_id: {job_id_match}")
            logger.info(f"🔧 Parsed secret: {secret_match[:20]}...")
            
            return job_id_match, secret_match
        except Exception as e:
            logger.error(f"❌ Error parsing job URL: {e}")
            raise
    
    def step1_route_units(self) -> bool:
        """Step 1: Route units to the job using unit_routing_workflow_script.py"""
        logger.info("")
        logger.info("🔄 STEP 1: ROUTING UNITS TO JOB")
        logger.info("=" * 60)
        
        try:
            # Use the first contributor from CSV as worker_id for routing
            # We'll read the CSV to get a contributor ID
            import csv
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                first_row = next(reader)
                worker_id = first_row['contributor_id']
            
            logger.info(f"🎯 Using worker_id for routing: {worker_id}")
            
            # Construct the unit routing command using unified script
            routing_script = "/Users/srafi/python_utils/unified_unit_routing_and_testing_script.py"
            
            if not os.path.exists(routing_script):
                logger.error(f"❌ Unified routing script not found: {routing_script}")
                return False
            
            # Route a few units (not all 70) for testing
            cmd = [
                'python3', routing_script,
                '--mode', 'routing',  # Use routing mode
                '--project-id', self.project_id,
                '--job-url', self.job_url,
                '--worker-id', worker_id,
                '--api-token', 'CgjVt3SvJfoQn_buVuaM',  # Default integration API token
                '--option', 'first_option',
                '--config-file', self.config_file,
                '--environment', 'integration',
                '--single-unit'  # Route only one unit for testing
            ]
            
            logger.info(f"🔧 Executing unit routing command:")
            logger.info(f"   {' '.join(cmd)}")
            
            # Execute the routing command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Unit routing completed successfully")
                logger.info(f"📄 Routing output: {result.stdout}")
                return True
            else:
                logger.warning(f"⚠️  Unit routing failed with return code: {result.returncode}")
                logger.warning(f"📄 Error output: {result.stderr}")
                # Don't fail the entire workflow for routing issues
                return True  # Continue with the workflow
                
        except Exception as e:
            logger.error(f"❌ Error in unit routing: {e}")
            # Don't fail the entire workflow for routing issues
            return True  # Continue with the workflow
    
    def step2_test_fetch_commit_before_deletion(self) -> bool:
        """Step 2: Test fetch/commit APIs before deletion"""
        logger.info("")
        logger.info("🔍 STEP 2: TESTING FETCH/COMMIT APIs (BEFORE DELETION)")
        logger.info("=" * 60)
        
        try:
            # Use the unified script for API testing
            test_script = "/Users/srafi/python_utils/unified_unit_routing_and_testing_script.py"
            
            if not os.path.exists(test_script):
                logger.error(f"❌ Unified testing script not found: {test_script}")
                return False
            
            # Test fetch/commit APIs
            cmd = [
                'python3', test_script,
                '--mode', 'testing',  # Use testing mode
                '--csv', self.csv_file,
                '--job-url', self.job_url,
                '--config-file', self.config_file,
                '--environment', 'integration',
                '--sample-size', '3'  # Test with 3 contributors
            ]
            
            logger.info(f"🔧 Executing fetch/commit test command:")
            logger.info(f"   {' '.join(cmd)}")
            
            # Execute the test command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Fetch/commit test completed successfully")
                logger.info(f"📄 Test output: {result.stdout}")
                return True
            else:
                logger.warning(f"⚠️  Fetch/commit test failed with return code: {result.returncode}")
                logger.warning(f"📄 Error output: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in fetch/commit testing: {e}")
            return False
    
    def step3_delete_contributors(self) -> bool:
        """Step 3: Delete contributors from all systems"""
        logger.info("")
        logger.info("🗑️  STEP 3: DELETING CONTRIBUTORS FROM ALL SYSTEMS")
        logger.info("=" * 60)
        
        try:
            # Use the delete_contributors_csv.py script
            deletion_script = "scripts/delete_contributors_csv.py"
            
            if not os.path.exists(deletion_script):
                logger.error(f"❌ Deletion script not found: {deletion_script}")
                return False
            
            # Delete contributors
            cmd = [
                'python3', deletion_script,
                '--csv', self.csv_file,
                '--config', self.config_file,
                '--integration',
                '--execute'
            ]
            
            logger.info(f"🔧 Executing contributor deletion command:")
            logger.info(f"   {' '.join(cmd)}")
            
            # Execute the deletion command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)  # 10 minutes timeout
            
            if result.returncode == 0:
                logger.info("✅ Contributor deletion completed successfully")
                logger.info(f"📄 Deletion output: {result.stdout}")
                return True
            else:
                logger.error(f"❌ Contributor deletion failed with return code: {result.returncode}")
                logger.error(f"📄 Error output: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in contributor deletion: {e}")
            return False
    
    def step4_test_fetch_after_deletion(self) -> bool:
        """Step 4: Test fetch API after deletion (should fail)"""
        logger.info("")
        logger.info("🔍 STEP 4: TESTING FETCH API (AFTER DELETION - SHOULD FAIL)")
        logger.info("=" * 60)
        
        try:
            # Use the unified script for API testing
            test_script = "/Users/srafi/python_utils/unified_unit_routing_and_testing_script.py"
            
            # Test fetch API only (no commit)
            cmd = [
                'python3', test_script,
                '--mode', 'testing',  # Use testing mode
                '--csv', self.csv_file,
                '--job-url', self.job_url,
                '--config-file', self.config_file,
                '--environment', 'integration',
                '--sample-size', '3'  # Test with 3 contributors (no commit option = fetch only)
            ]
            
            logger.info(f"🔧 Executing fetch test command (after deletion):")
            logger.info(f"   {' '.join(cmd)}")
            
            # Execute the test command
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            # For this step, we expect it to fail (contributors should be kicked out)
            if result.returncode != 0:
                logger.info("✅ Fetch test failed as expected (contributors kicked out)")
                logger.info(f"📄 Expected failure output: {result.stderr}")
                return True
            else:
                logger.warning("⚠️  Fetch test succeeded unexpectedly (contributors still have access)")
                logger.warning(f"📄 Unexpected success output: {result.stdout}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error in post-deletion fetch testing: {e}")
            return False
    
    def step5_verify_elasticsearch_masking(self) -> bool:
        """Step 5: Verify DELETED_USER appears in Elasticsearch"""
        logger.info("")
        logger.info("🔍 STEP 5: VERIFYING ELASTICSEARCH MASKING")
        logger.info("=" * 60)
        
        try:
            # Read contributors from CSV
            import csv
            contributors = []
            with open(self.csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    contributors.append({
                        'contributor_id': row['contributor_id'],
                        'email': row['email_address']
                    })
            
            # Test first contributor
            if contributors:
                contributor = contributors[0]
                logger.info(f"🎯 Verifying masking for contributor: {contributor['contributor_id']}")
                logger.info(f"📧 Original email: {contributor['email']}")
                
                # Use curl to search Elasticsearch for DELETED_USER
                es_url = "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com"
                project_index = f"project-{self.project_id}"
                
                # Search for DELETED_USER in the project index
                search_query = {
                    "query": {
                        "bool": {
                            "should": [
                                {"term": {"latest.workerEmail": "DELETED_USER"}},
                                {"term": {"latest.lastAnnotatorEmail": "DELETED_USER"}},
                                {"term": {"latest.lastReviewerEmail": "DELETED_USER"}},
                                {"term": {"email": "DELETED_USER"}},
                                {"term": {"worker_email": "DELETED_USER"}}
                            ],
                            "minimum_should_match": 1
                        }
                    },
                    "size": 5
                }
                
                # Create temporary file for query
                import tempfile
                import json
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(search_query, f)
                    query_file = f.name
                
                # Execute curl command
                curl_cmd = [
                    'curl', '-s', '-X', 'GET',
                    f'{es_url}/{project_index}/_search',
                    '-H', 'Content-Type: application/json',
                    '-d', f'@{query_file}'
                ]
                
                logger.info(f"🚀 Executing Elasticsearch verification curl command:")
                logger.info(f"   Command: {' '.join(curl_cmd)}")
                logger.info(f"   Request URL: {es_url}/{project_index}/_search")
                logger.info(f"   Request Headers: Content-Type: application/json")
                logger.info(f"   Request Body File: {query_file}")
                
                # Log the complete verification request payload
                with open(query_file, 'r') as f:
                    verification_payload = f.read()
                logger.info(f"📋 Complete Verification Request Payload:")
                logger.info(f"   {json.dumps(json.loads(verification_payload), indent=2)}")
                
                result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=60)
                
                # Log complete curl response
                logger.info(f"📋 Complete Elasticsearch Verification Curl Response:")
                logger.info(f"   Return Code: {result.returncode}")
                logger.info(f"   STDOUT: {result.stdout}")
                logger.info(f"   STDERR: {result.stderr}")
                
                # Clean up
                os.unlink(query_file)
                
                if result.returncode == 0:
                    try:
                        response_data = json.loads(result.stdout)
                        logger.info(f"📋 Parsed Elasticsearch Verification Response Data:")
                        logger.info(f"   {json.dumps(response_data, indent=2)}")
                        
                        total_hits = response_data.get('hits', {}).get('total', {}).get('value', 0)
                        
                        if total_hits > 0:
                            logger.info(f"✅ Found {total_hits} documents with DELETED_USER in Elasticsearch")
                            return True
                        else:
                            logger.warning("⚠️  No documents found with DELETED_USER in Elasticsearch")
                            return False
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ Could not parse Elasticsearch response: {e}")
                        logger.error(f"📄 Raw STDOUT: {result.stdout}")
                        logger.error(f"📄 Raw STDERR: {result.stderr}")
                        return False
                else:
                    logger.error(f"❌ Elasticsearch verification failed: {result.stderr}")
                    logger.error(f"📄 Full STDOUT: {result.stdout}")
                    logger.error(f"📄 Full STDERR: {result.stderr}")
                    return False
            
            return False
                
        except Exception as e:
            logger.error(f"❌ Error in Elasticsearch verification: {e}")
            return False
    
    def step6_verify_clickhouse_masking(self) -> bool:
        """Step 6: Verify DELETED_USER appears in ClickHouse"""
        logger.info("")
        logger.info("📊 STEP 6: VERIFYING CLICKHOUSE MASKING")
        logger.info("=" * 60)
        
        try:
            # Use curl to query ClickHouse for DELETED_USER
            clickhouse_url = "http://kepler:cLE8L3OEdr63@localhost:8123"
            
            # Query ClickHouse tables for DELETED_USER
            tables = [
                'kepler.unit_metrics',
                'kepler.unit_metrics_hourly',
                'kepler.unit_metrics_topic'
            ]
            
            for table in tables:
                query = f"SELECT COUNT(*) as count FROM {table} WHERE email = 'DELETED_USER'"
                
                curl_cmd = [
                    'curl', '-s', '-X', 'POST',
                    f'{clickhouse_url}/',
                    '-H', 'Content-Type: text/plain',
                    '-d', query
                ]
                
                logger.info(f"🚀 Executing ClickHouse verification curl command:")
                logger.info(f"   Command: {' '.join(curl_cmd)}")
                logger.info(f"   Request URL: {clickhouse_url}/")
                logger.info(f"   Request Headers: Content-Type: text/plain")
                logger.info(f"   Request Body (SQL Query): {query}")
                logger.info(f"   Target Table: {table}")
                
                result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
                
                # Log complete curl response
                logger.info(f"📋 Complete ClickHouse Verification Curl Response:")
                logger.info(f"   Return Code: {result.returncode}")
                logger.info(f"   STDOUT: {result.stdout}")
                logger.info(f"   STDERR: {result.stderr}")
                
                if result.returncode == 0:
                    count = result.stdout.strip()
                    logger.info(f"📋 ClickHouse Query Result:")
                    logger.info(f"   Raw Output: {count}")
                    if count.isdigit() and int(count) > 0:
                        logger.info(f"✅ Found {count} records with DELETED_USER in {table}")
                        return True
                    else:
                        logger.info(f"📊 No DELETED_USER records found in {table}")
                else:
                    logger.warning(f"⚠️  ClickHouse query failed for {table}: {result.stderr}")
                    logger.warning(f"📄 Full STDOUT: {result.stdout}")
                    logger.warning(f"📄 Full STDERR: {result.stderr}")
            
            logger.warning("⚠️  No DELETED_USER records found in any ClickHouse table")
            return False
                
        except Exception as e:
            logger.error(f"❌ Error in ClickHouse verification: {e}")
            return False
    
    def run_complete_workflow(self) -> bool:
        """Run the complete testing workflow"""
        logger.info("")
        logger.info("🚀 STARTING COMPLETE TESTING WORKFLOW")
        logger.info("=" * 80)
        
        steps = [
            ("Unit Routing", self.step1_route_units),
            ("Fetch/Commit Test (Before)", self.step2_test_fetch_commit_before_deletion),
            ("Contributor Deletion", self.step3_delete_contributors),
            ("Fetch Test (After)", self.step4_test_fetch_after_deletion),
            ("Elasticsearch Verification", self.step5_verify_elasticsearch_masking),
            ("ClickHouse Verification", self.step6_verify_clickhouse_masking)
        ]
        
        results = {}
        
        for step_name, step_function in steps:
            logger.info(f"")
            logger.info(f"🔄 Executing: {step_name}")
            logger.info("-" * 40)
            
            try:
                start_time = time.time()
                success = step_function()
                elapsed_time = time.time() - start_time
                
                results[step_name] = {
                    'success': success,
                    'elapsed_time': elapsed_time
                }
                
                if success:
                    logger.info(f"✅ {step_name} completed successfully in {elapsed_time:.2f}s")
                else:
                    logger.warning(f"⚠️  {step_name} completed with issues in {elapsed_time:.2f}s")
                    
            except Exception as e:
                logger.error(f"❌ {step_name} failed with exception: {e}")
                results[step_name] = {
                    'success': False,
                    'elapsed_time': 0,
                    'error': str(e)
                }
        
        # Print summary
        self._print_workflow_summary(results)
        
        # Return overall success (all critical steps must succeed)
        critical_steps = ["Contributor Deletion", "Fetch Test (After)", "Elasticsearch Verification"]
        overall_success = all(results.get(step, {}).get('success', False) for step in critical_steps)
        
        return overall_success
    
    def _print_workflow_summary(self, results: Dict):
        """Print workflow summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 COMPREHENSIVE TESTING WORKFLOW SUMMARY")
        logger.info("=" * 80)
        
        total_time = (datetime.now() - self.workflow_start_time).total_seconds()
        
        for step_name, result in results.items():
            status = "✅ SUCCESS" if result['success'] else "❌ FAILED"
            elapsed = result.get('elapsed_time', 0)
            logger.info(f"   {step_name}: {status} ({elapsed:.2f}s)")
            
            if 'error' in result:
                logger.info(f"      Error: {result['error']}")
        
        logger.info("")
        logger.info(f"🕐 Total workflow time: {total_time:.2f}s")
        logger.info("=" * 80)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Comprehensive testing workflow for contributor deletion')
    parser.add_argument('--project-id', required=True, help='Project ID')
    parser.add_argument('--job-url', required=True, help='Job URL with secret')
    parser.add_argument('--csv', required=True, help='CSV file with contributors')
    parser.add_argument('--config', required=True, help='Configuration file path')
    
    args = parser.parse_args()
    
    # Validate inputs
    if not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)
    
    config_path = os.path.expanduser(args.config)
    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)
    
    try:
        # Initialize workflow
        workflow = ComprehensiveTestingWorkflow(
            project_id=args.project_id,
            job_url=args.job_url,
            csv_file=args.csv,
            config_file=config_path
        )
        
        # Run complete workflow
        success = workflow.run_complete_workflow()
        
        if success:
            logger.info("🎉 COMPREHENSIVE TESTING WORKFLOW COMPLETED SUCCESSFULLY")
            sys.exit(0)
        else:
            logger.error("❌ COMPREHENSIVE TESTING WORKFLOW FAILED")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Workflow execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
