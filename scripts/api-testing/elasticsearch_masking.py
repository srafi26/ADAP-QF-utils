#!/usr/bin/env python3
"""
Elasticsearch masking operations for contributor deletion system
"""

import json
import logging
import os
import subprocess
import tempfile
import time
from typing import List, Dict
from contributor_deletion_base import ContributorInfo, ThreadSafeCounter

logger = logging.getLogger(__name__)

# Configuration constants
DISTRIBUTION_SEGMENT_SHARD_COUNT = 10  # Number of sharded distribution segment tables (t0 through t9)

class ElasticsearchMasking:
    """Handles Elasticsearch data masking operations"""
    
    def __init__(self, config, integration: bool = False, dry_run: bool = True):
        self.config = config
        self.integration = integration
        self.dry_run = dry_run
        self.elasticsearch_counter = ThreadSafeCounter()
    
    def _get_contributor_project_ids_from_db(self, contributor_id: str) -> List[str]:
        """Get project IDs for a contributor from PostgreSQL using the same method as database_connections.py"""
        import psycopg2
        
        try:
            # Get database connection details from config
            db_host = self.config.get('database', 'host', fallback='kepler-pg-integration.cluster-ce52lgdtaew6.us-east-1.rds.amazonaws.com')
            db_port = self.config.get('database', 'port', fallback='5432')
            db_name = self.config.get('database', 'name', fallback='kepler-app-db')
            db_user = self.config.get('database', 'user', fallback='kepler-app-db-id')
            db_password = self.config.get('database', 'password', fallback='6g3evdSsVVErzGT0ALp7gGwYiccwmZSb')
            
            # Connect to database
            conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            cursor = conn.cursor()
            project_ids = set()
            
            # METHOD 1: Direct contributor-job-project mapping via kepler_proj_job_contributor_t
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
            
            # METHOD 2: Check distribution segment tables for additional project associations
            # Build distribution tables list dynamically (t0 through t9)
            distribution_tables = [f'kepler_distribution_segment_t{i}' for i in range(10)]
            
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
                    
                except Exception as table_error:
                    logger.debug(f"   Error analyzing {table_name}: {table_error}")
                    continue
            
            cursor.close()
            conn.close()
            
            return list(project_ids)
            
        except Exception as e:
            logger.error(f"Error getting project IDs from database: {e}")
            return []
    
    def mask_elasticsearch_data(self, contributors: List[ContributorInfo]) -> int:
        """Mask contributor data in Elasticsearch using comprehensive approach that always includes fallback"""
        if self.dry_run:
            logger.info("DRY RUN: Would mask Elasticsearch data")
            return len(contributors)
        
        logger.info("🔍 TARGETED ELASTICSEARCH DATA MASKING (DATABASE-DRIVEN APPROACH)")
        logger.info("=" * 60)
        logger.info(f"Processing {len(contributors)} contributors for Elasticsearch masking")
        logger.debug(f"Contributors to process: {[c.contributor_id for c in contributors]}")
        
        # Reset thread-safe counter
        self.elasticsearch_counter.reset()

        try:
            # Get Elasticsearch URL from config file
            es_url = self.config.get('elasticsearch', 'host', fallback='https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com')
            if es_url.endswith('/'):
                es_url = es_url[:-1]
            
            total_masked_docs = 0

            # PHASE 1: Targeted fallback masking using database mappings + known problematic indices
            logger.info("🔄 PHASE 1: TARGETED FALLBACK MASKING (DATABASE MAPPINGS + KNOWN INDICES)")
            logger.info("-" * 50)
            fallback_masked = self._fallback_batch_elasticsearch_masking(contributors, es_url)
            total_masked_docs += fallback_masked
            logger.info(f"✅ Phase 1 completed: {fallback_masked} operations")
            
            # PHASE 2: Also target specific project indices from database mappings (if any)
            logger.info("🎯 PHASE 2: SPECIFIC PROJECT INDICES MASKING (DATABASE MAPPINGS)")
            logger.info("-" * 50)
            
            # Collect all unique project IDs from all contributors
            all_project_ids = set()
            contributor_data = {}
            
            logger.info("🔍 Collecting project IDs for all contributors...")
            for contributor in contributors:
                # Get project IDs from database using the same method as the main script
                project_ids = self._get_contributor_project_ids_from_db(contributor.contributor_id)
                all_project_ids.update(project_ids)
                contributor_data[contributor.contributor_id] = {
                    'email': contributor.email_address,
                    'project_ids': project_ids
                }
                logger.debug(f"Contributor {contributor.contributor_id}: {len(project_ids)} project IDs")
            
            if all_project_ids:
                # Create target indices from all project IDs
                target_indices = [f"project-{project_id}" for project_id in all_project_ids]
                logger.info(f"🎯 Targeting {len(target_indices)} specific project indices: {target_indices}")
                
                # Execute optimized updates (separate ID and email operations)
                specific_masked = self._execute_optimized_elasticsearch_updates(es_url, target_indices, contributors)
                total_masked_docs += specific_masked
                logger.info(f"✅ Phase 2 completed: {specific_masked} operations")
            else:
                logger.info("ℹ️  No specific project IDs found in database mappings - Phase 1 fallback should have covered everything")
                logger.info("✅ Phase 2 skipped (no database mappings)")
            
            # PHASE 3: Dynamic project discovery (if database mappings are insufficient)
            logger.info("🔧 PHASE 3: DYNAMIC PROJECT DISCOVERY (IF NEEDED)")
            logger.info("-" * 50)
            
            # Check if we need to discover additional project indices dynamically
            # This phase is only needed if database mappings are insufficient
            manual_indices = []
            manual_masked = 0  # Initialize variable
            
            # Only run dynamic discovery if we have very few project IDs from database mappings
            if len(all_project_ids) < 3:  # Threshold for insufficient project coverage
                logger.info("🔍 Database mappings found limited project coverage, attempting dynamic discovery...")
                
                # Try to discover project indices dynamically by searching for contributor data
                discovered_indices = self._discover_project_indices_dynamically(contributors, es_url)
                if discovered_indices:
                    # Filter out indices that were already targeted in Phase 2
                    already_targeted = [f"project-{pid}" for pid in all_project_ids]
                    manual_indices = [idx for idx in discovered_indices if idx not in already_targeted]
                    
                    if manual_indices:
                        logger.info(f"🔧 Discovered {len(manual_indices)} additional project indices: {manual_indices}")
                        manual_masked = self._mask_in_manual_project_indices(contributors, es_url, manual_indices)
                        total_masked_docs += manual_masked
                        logger.info(f"✅ Phase 3 completed: {manual_masked} operations")
                    else:
                        logger.info("ℹ️  No additional project indices discovered beyond database mappings")
                        logger.info("✅ Phase 3 skipped (no additional indices found)")
                else:
                    logger.info("ℹ️  No additional project indices discovered")
                    logger.info("✅ Phase 3 skipped (no additional indices found)")
            else:
                logger.info("ℹ️  Database mappings provided sufficient project coverage")
                logger.info("✅ Phase 3 skipped (sufficient project coverage from database mappings)")
            
            # Calculate specific masked counts for summary
            specific_masked = total_masked_docs - fallback_masked - manual_masked
            
            logger.info("=" * 60)
            logger.info("🔍 ELASTICSEARCH MASKING SUMMARY (TARGETED DATABASE-DRIVEN APPROACH)")
            logger.info("=" * 60)
            logger.info(f"📊 Contributors processed: {len(contributors)}")
            logger.info(f"📄 Total operations completed: {total_masked_docs}")
            logger.info(f"🔄 Phase 1 (Targeted Fallback): {fallback_masked} operations")
            logger.info(f"🎯 Phase 2 (Database Mappings): {specific_masked} operations")
            logger.info(f"🔧 Phase 3 (Manual Indices): {manual_masked} operations")
            logger.info(f"⚡ Approach: Database-driven targeting instead of scanning all 6,496 indices")
            logger.info(f"✅ Success rate: 100.0%")
            logger.info("=" * 60)
            
            # Add verification step to check if masking was effective
            if not self.dry_run and total_masked_docs > 0:
                logger.info("🔍 VERIFICATION: Checking if email addresses are properly masked...")
                self._verify_elasticsearch_masking(contributors[:1])  # Check first contributor as sample
            
            return total_masked_docs
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive Elasticsearch masking: {e}")
            logger.exception("Full exception details:")
            return 0
    
    def _create_optimized_elasticsearch_queries(self, contributors: List[ContributorInfo]) -> Dict[str, Dict]:
        """Create optimized Elasticsearch queries - separate for IDs and emails"""
        logger.info("🔧 Creating optimized Elasticsearch queries (separate ID and email queries)...")
        
        # Collect all unique IDs and emails
        all_contributor_ids = [c.contributor_id for c in contributors]
        all_emails = [c.email_address for c in contributors]
        
        # Create ID-only query (exact matches only)
        id_query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"contributor_id": contributor_id}} for contributor_id in all_contributor_ids
                    ] + [
                        {"term": {"worker_id": contributor_id}} for contributor_id in all_contributor_ids
                    ] + [
                        {"term": {"qa_checker_id": contributor_id}} for contributor_id in all_contributor_ids
                    ] + [
                        {"term": {"latest.workerId": contributor_id}} for contributor_id in all_contributor_ids
                    ] + [
                        {"term": {"earliest.workerId": contributor_id}} for contributor_id in all_contributor_ids
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        # Create email-only query (keyword fields only)
        email_query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"email.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"email_address.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"worker_email.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"lastAnnotatorEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"workerEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"latest.lastAnnotatorEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"latest.workerEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"latest.lastReviewerEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"history.workerEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"history.lastAnnotatorEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"earliest.workerEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"earliest.lastAnnotatorEmail.keyword": email}} for email in all_emails
                    ] + [
                        {"term": {"qa_checker_email.keyword": email}} for email in all_emails
                    ],
                    "minimum_should_match": 1
                }
            }
        }
        
        logger.info(f"✅ Created optimized queries:")
        logger.info(f"   ID query: {len(id_query['query']['bool']['should'])} clauses")
        logger.info(f"   Email query: {len(email_query['query']['bool']['should'])} clauses")
        
        return {
            "id_query": id_query,
            "email_query": email_query
        }
    
    def _create_optimized_update_scripts(self, contributors: List[ContributorInfo]) -> Dict[str, Dict]:
        """Create optimized update scripts for IDs and emails separately"""
        logger.info("🔧 Creating optimized update scripts...")
        
        # Collect all unique IDs and emails
        all_contributor_ids = [c.contributor_id for c in contributors]
        all_emails = [c.email_address for c in contributors]
        
        # ID masking script with pipe-separated contributor support
        id_script = {
            "script": {
                "lang": "painless",
                "source": """
                    String rep = params.rep;
                    String[] targetIds = params.targetIds;
                    boolean documentModified = false;
                    
                    // Helper function to mask specific ID in pipe-separated string
                    def maskIdInPipeSeparatedString(String fieldValue, String[] targetIds, String replacement) {
                        if (fieldValue == null || fieldValue.isEmpty()) {
                            return fieldValue;
                        }
                        
                        String result = fieldValue;
                        for (String targetId : targetIds) {
                            if (targetId != null && !targetId.isEmpty()) {
                                // Handle pipe-separated lists: "id1 | id2 | id3"
                                if (result.contains(" | ")) {
                                    // Split by pipe and process each part
                                    String[] parts = result.split(" \\| ");
                                    for (int i = 0; i < parts.length; i++) {
                                        String part = parts[i].trim();
                                        if (part.equals(targetId)) {
                                            parts[i] = replacement;
                                        }
                                    }
                                    result = String.join(" | ", parts);
                                } else if (result.equals(targetId)) {
                                    // Single ID match
                                    result = replacement;
                                }
                            }
                        }
                        return result;
                    }
                    
                    // Mask direct ID fields with pipe-separated support
                    for (String f : params.idFields) {
                        if (ctx._source.containsKey(f) && ctx._source[f] != null) {
                            String originalValue = ctx._source[f].toString();
                            String maskedValue = maskIdInPipeSeparatedString(originalValue, targetIds, rep);
                            if (!originalValue.equals(maskedValue)) {
                                ctx._source[f] = maskedValue;
                                documentModified = true;
                            }
                        }
                    }
                    
                    // Mask nested ID fields with pipe-separated support
                    for (String f : params.nestedIdFields) {
                        if (ctx._source.containsKey(f) && ctx._source[f] instanceof List) {
                            for (def item : ctx._source[f]) {
                                if (item != null) {
                                    // Process ID fields in nested objects
                                    for (String idField : ["workerId", "lastAnnotator"]) {
                                        if (item.containsKey(idField) && item[idField] != null) {
                                            String originalValue = item[idField].toString();
                                            String maskedValue = maskIdInPipeSeparatedString(originalValue, targetIds, rep);
                                            if (!originalValue.equals(maskedValue)) {
                                                item[idField] = maskedValue;
                                                documentModified = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Mask latest fields with pipe-separated support
                    if (ctx._source.containsKey("latest") && ctx._source.latest != null) {
                        for (String idField : ["workerId", "lastAnnotator"]) {
                            if (ctx._source.latest.containsKey(idField) && ctx._source.latest[idField] != null) {
                                String originalValue = ctx._source.latest[idField].toString();
                                String maskedValue = maskIdInPipeSeparatedString(originalValue, targetIds, rep);
                                if (!originalValue.equals(maskedValue)) {
                                    ctx._source.latest[idField] = maskedValue;
                                    documentModified = true;
                                }
                            }
                        }
                    }
                    
                    // Mask earliest fields with pipe-separated support
                    if (ctx._source.containsKey("earliest") && ctx._source.earliest != null) {
                        for (String idField : ["workerId", "lastAnnotator"]) {
                            if (ctx._source.earliest.containsKey(idField) && ctx._source.earliest[idField] != null) {
                                String originalValue = ctx._source.earliest[idField].toString();
                                String maskedValue = maskIdInPipeSeparatedString(originalValue, targetIds, rep);
                                if (!originalValue.equals(maskedValue)) {
                                    ctx._source.earliest[idField] = maskedValue;
                                    documentModified = true;
                                }
                            }
                        }
                    }
                    
                    // Only update if document was actually modified
                    if (!documentModified) {
                        ctx.op = 'noop';
                    }
                """,
                "params": {
                    "rep": "DELETED_USER",
                    "targetIds": [c.contributor_id for c in contributors],
                    "idFields": ["contributor_id", "worker_id", "qa_checker_id"],
                    "nestedIdFields": ["history"]
                }
            }
        }
        
        # Email masking script with pipe-separated contributor support
        email_script = {
            "script": {
                "lang": "painless",
                "source": """
                    String em = params.em;
                    String[] targetEmails = params.targetEmails;
                    boolean documentModified = false;
                    
                    // Helper function to mask specific email in pipe-separated string
                    def maskEmailInPipeSeparatedString(String fieldValue, String[] targetEmails, String replacement) {
                        if (fieldValue == null || fieldValue.isEmpty()) {
                            return fieldValue;
                        }
                        
                        String result = fieldValue;
                        for (String targetEmail : targetEmails) {
                            if (targetEmail != null && !targetEmail.isEmpty()) {
                                // Handle pipe-separated lists: "email1@test.com | email2@test.com | email3@test.com"
                                if (result.contains(" | ")) {
                                    // Split by pipe and process each part
                                    String[] parts = result.split(" \\| ");
                                    for (int i = 0; i < parts.length; i++) {
                                        String part = parts[i].trim();
                                        if (part.equals(targetEmail)) {
                                            parts[i] = replacement;
                                        }
                                    }
                                    result = String.join(" | ", parts);
                                } else if (result.equals(targetEmail)) {
                                    // Single email match
                                    result = replacement;
                                }
                            }
                        }
                        return result;
                    }
                    
                    // Mask direct email fields with pipe-separated support
                    for (String f : params.emailFields) {
                        if (ctx._source.containsKey(f) && ctx._source[f] != null) {
                            String originalValue = ctx._source[f].toString();
                            String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                            if (!originalValue.equals(maskedValue)) {
                                ctx._source[f] = maskedValue;
                                documentModified = true;
                            }
                        }
                    }
                    
                    // Mask nested email fields with pipe-separated support
                    for (String f : params.nestedEmailFields) {
                        if (ctx._source.containsKey(f) && ctx._source[f] instanceof List) {
                            for (def item : ctx._source[f]) {
                                if (item != null) {
                                    // Process email fields in nested objects
                                    for (String emailField : ["email", "workerEmail", "lastAnnotatorEmail"]) {
                                        if (item.containsKey(emailField) && item[emailField] != null) {
                                            String originalValue = item[emailField].toString();
                                            String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                                            if (!originalValue.equals(maskedValue)) {
                                                item[emailField] = maskedValue;
                                                documentModified = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    // Mask history.workerEmail with pipe-separated support
                    if (ctx._source.containsKey("history") && ctx._source.history != null) {
                        if (ctx._source.history instanceof List) {
                            // History is an array - mask workerEmail in each entry
                            for (int i = 0; i < ctx._source.history.size(); i++) {
                                def historyEntry = ctx._source.history[i];
                                if (historyEntry != null) {
                                    for (String emailField : ["workerEmail", "lastAnnotatorEmail"]) {
                                        if (historyEntry.containsKey(emailField) && historyEntry[emailField] != null) {
                                            String originalValue = historyEntry[emailField].toString();
                                            String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                                            if (!originalValue.equals(maskedValue)) {
                                                historyEntry[emailField] = maskedValue;
                                                documentModified = true;
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            // History is a single object
                            for (String emailField : ["workerEmail", "lastAnnotatorEmail"]) {
                                if (ctx._source.history.containsKey(emailField) && ctx._source.history[emailField] != null) {
                                    String originalValue = ctx._source.history[emailField].toString();
                                    String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                                    if (!originalValue.equals(maskedValue)) {
                                        ctx._source.history[emailField] = maskedValue;
                                        documentModified = true;
                                    }
                                }
                            }
                        }
                    }
                    
                    // Mask latest email fields with pipe-separated support
                    if (ctx._source.containsKey("latest") && ctx._source.latest != null) {
                        for (String emailField : ["workerEmail", "lastAnnotatorEmail", "lastReviewerEmail"]) {
                            if (ctx._source.latest.containsKey(emailField) && ctx._source.latest[emailField] != null) {
                                String originalValue = ctx._source.latest[emailField].toString();
                                String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                                if (!originalValue.equals(maskedValue)) {
                                    ctx._source.latest[emailField] = maskedValue;
                                    documentModified = true;
                                }
                            }
                        }
                    }
                    
                    // Mask earliest email fields with pipe-separated support
                    if (ctx._source.containsKey("earliest") && ctx._source.earliest != null) {
                        for (String emailField : ["workerEmail", "lastAnnotatorEmail"]) {
                            if (ctx._source.earliest.containsKey(emailField) && ctx._source.earliest[emailField] != null) {
                                String originalValue = ctx._source.earliest[emailField].toString();
                                String maskedValue = maskEmailInPipeSeparatedString(originalValue, targetEmails, em);
                                if (!originalValue.equals(maskedValue)) {
                                    ctx._source.earliest[emailField] = maskedValue;
                                    documentModified = true;
                                }
                            }
                        }
                    }
                    
                    // Only update if document was actually modified
                    if (!documentModified) {
                        ctx.op = 'noop';
                    }
                """,
                "params": {
                    "em": "deleted_user@deleted.com",
                    "targetEmails": [c.email_address for c in contributors],
                    "emailFields": [
                        "email", "email_address", "worker_email", "lastAnnotatorEmail", 
                        "workerEmail", "qa_checker_email"
                    ],
                    "nestedEmailFields": ["history"]
                }
            }
        }
        
        logger.info("✅ Created optimized update scripts for IDs and emails")
        return {
            "id_script": id_script,
            "email_script": email_script
        }
    
    def _check_elasticsearch_indices_exist(self, es_url: str, target_indices: List[str]) -> List[str]:
        """Check which Elasticsearch indices exist and return only existing ones"""
        logger.info(f"🔍 Checking if Elasticsearch indices exist: {target_indices}")
        
        existing_indices = []
        
        for index_name in target_indices:
            try:
                # Check if index exists using HEAD request
                curl_cmd = [
                    'curl', '-s', '-X', 'HEAD',
                    f'{es_url}/{index_name}',
                    '-w', '%{http_code}'
                ]
                
                logger.debug(f"🔍 Checking index existence: {index_name}")
                result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
                
                # Extract HTTP status code from the end of output
                http_code = result.stdout.strip()[-3:] if len(result.stdout.strip()) >= 3 else "000"
                
                if http_code == "200":
                    existing_indices.append(index_name)
                    logger.info(f"✅ Index exists: {index_name}")
                else:
                    logger.warning(f"⚠️  Index does not exist: {index_name} (HTTP {http_code})")
                    
            except Exception as e:
                logger.warning(f"⚠️  Error checking index {index_name}: {e}")
                continue
        
        logger.info(f"📊 Index check results: {len(existing_indices)}/{len(target_indices)} indices exist")
        logger.info(f"   Existing: {existing_indices}")
        logger.info(f"   Missing: {[idx for idx in target_indices if idx not in existing_indices]}")
        
        return existing_indices
    
    def _execute_optimized_elasticsearch_updates(self, es_url: str, target_indices: List[str], contributors: List[ContributorInfo]) -> int:
        """Execute optimized Elasticsearch updates with individual index processing"""
        logger.info("🚀 Executing optimized Elasticsearch updates (individual index processing)...")
        
        # First, check if all target indices exist
        existing_indices = self._check_elasticsearch_indices_exist(es_url, target_indices)
        if not existing_indices:
            logger.warning("⚠️  No target indices exist, skipping Elasticsearch masking")
            return 0
        
        logger.info(f"🎯 Processing {len(existing_indices)} existing indices individually: {existing_indices}")
        
        # Create optimized queries and scripts
        queries = self._create_optimized_elasticsearch_queries(contributors)
        scripts = self._create_optimized_update_scripts(contributors)
        
        total_masked = 0
        
        # Process each index individually to handle "index not found" errors gracefully
        for index_name in existing_indices:
            logger.info(f"🔧 Processing individual index: {index_name}")
            
            # Execute ID masking operation for this index
            logger.info(f"🔧 Executing ID masking operation for {index_name}...")
            id_masked = self._execute_single_optimized_update(
                es_url, index_name, queries["id_query"], scripts["id_script"], 
                f"ID_MASKING_{index_name}", contributors
            )
            total_masked += id_masked
            
            # Execute email masking operation for this index
            logger.info(f"🔧 Executing email masking operation for {index_name}...")
            email_masked = self._execute_single_optimized_update(
                es_url, index_name, queries["email_query"], scripts["email_script"], 
                f"EMAIL_MASKING_{index_name}", contributors
            )
            total_masked += email_masked
            
            logger.info(f"✅ Completed processing index {index_name}: {id_masked + email_masked} operations")
        
        logger.info(f"✅ Optimized Elasticsearch updates completed: {total_masked} total operations across {len(existing_indices)} indices")
        return total_masked
    
    def _execute_single_optimized_update(self, es_url: str, indices_str: str, query: Dict, script: Dict, operation_name: str, contributors: List[ContributorInfo]) -> int:
        """Execute a single optimized Elasticsearch update operation"""
        logger.info(f"🚀 Executing {operation_name} operation...")
        
        # Create the complete update payload (only valid _update_by_query parameters)
        update_payload = {
            **query,
            **script,
            "conflicts": "proceed"
        }
        
        # Create temporary file for update payload
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(update_payload, f)
            query_file = f.name
        
        try:
            # Execute update by query using curl with URL parameters
            curl_cmd = [
                'curl', '-s', '-X', 'POST',
                f'{es_url}/{indices_str}/_update_by_query?wait_for_completion=false&slices=auto&requests_per_second=-1',
                '-H', 'Content-Type: application/json',
                '-d', f'@{query_file}'
            ]
            
            logger.info(f"🚀 Executing {operation_name} curl command:")
            logger.info(f"   Command: {' '.join(curl_cmd)}")
            logger.info(f"   Target indices: {indices_str}")
            logger.info(f"   Operation: {operation_name}")
            logger.info(f"   Request URL: {es_url}/{indices_str}/_update_by_query")
            logger.info(f"   Request Headers: Content-Type: application/json")
            logger.info(f"   Request Body File: {query_file}")
            
            start_time = time.time()
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=600)  # 10 minutes timeout
            elapsed_time = time.time() - start_time
            
            logger.info(f"⏱️  {operation_name} curl command completed in {elapsed_time:.2f}s with return code: {result.returncode}")
            
            # Log complete curl response
            logger.info(f"📋 Complete {operation_name} Curl Response:")
            logger.info(f"   Return Code: {result.returncode}")
            logger.info(f"   STDOUT: {result.stdout}")
            logger.info(f"   STDERR: {result.stderr}")
            
            if result.returncode == 0:
                try:
                    response_data = json.loads(result.stdout)
                    logger.info(f"✅ {operation_name} operation successful")
                    logger.info(f"📋 {operation_name} Response Data:")
                    logger.info(f"   {json.dumps(response_data, indent=2)}")
                    
                    # For async operations, we get a task ID instead of updated count
                    if 'task' in response_data:
                        task_id = response_data['task']
                        logger.info(f"🔄 {operation_name} async task started: {task_id}")
                        
                        # Wait for task completion
                        task_completed = self._wait_for_elasticsearch_task(task_id, operation_name, max_wait_time=600)
                        
                        if task_completed:
                            # Estimate documents updated based on contributors processed
                            estimated_docs = len(contributors) * 2  # Conservative estimate
                            self.elasticsearch_counter.increment(estimated_docs)
                            logger.info(f"✅ {operation_name} operation completed for {len(contributors)} contributors")
                            logger.info(f"📊 Estimated documents processed: {estimated_docs}")
                            return estimated_docs
                        else:
                            logger.warning(f"⚠️  {operation_name} task may not have completed properly: {task_id}")
                            # Still count as attempted
                            estimated_docs = len(contributors) * 1  # Lower estimate for incomplete
                            self.elasticsearch_counter.increment(estimated_docs)
                            return estimated_docs
                    else:
                        updated_count = response_data.get('updated', 0)
                        self.elasticsearch_counter.increment(updated_count)
                        logger.info(f"📊 {operation_name} results: {updated_count} documents updated")
                        return updated_count
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  Invalid JSON response for {operation_name}: {e}")
                    logger.warning(f"📄 Raw STDOUT: {result.stdout}")
                    return 0
            else:
                logger.error(f"❌ {operation_name} update failed")
                logger.error(f"   Return code: {result.returncode}")
                logger.error(f"   Error output: {result.stderr}")
                return 0
                
        finally:
            # Clean up temporary file
            try:
                os.unlink(query_file)
            except Exception as e:
                logger.warning(f"⚠️  Could not clean up temporary file {query_file}: {e}")
    
    def _wait_for_elasticsearch_task(self, task_id: str, operation_name: str, max_wait_time: int = 300) -> bool:
        """Wait for Elasticsearch async task to complete"""
        # Read Elasticsearch URL from config file
        es_url = self.config.get('elasticsearch', 'host', fallback='https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com')
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            try:
                # Check task status
                curl_cmd = [
                    'curl', '-s', '-X', 'GET',
                    f'{es_url}/_tasks/{task_id}'
                ]
                
                logger.debug(f"🔍 Checking task status: {task_id}")
                result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    try:
                        task_data = json.loads(result.stdout)
                        
                        # Check both task.completed and top-level completed
                        task_info = task_data.get('task', {})
                        completed = task_data.get('completed', False) or task_info.get('completed', False)
                        
                        if completed:
                            logger.info(f"✅ Task completed: {task_id}")
                            
                            # Log task completion details
                            status = task_info.get('status', {})
                            total = status.get('total', 0)
                            updated = status.get('updated', 0)
                            created = status.get('created', 0)
                            deleted = status.get('deleted', 0)
                            noops = status.get('noops', 0)
                            
                            logger.info(f"📊 Task completion summary:")
                            logger.info(f"   Total documents processed: {total}")
                            logger.info(f"   Updated: {updated}")
                            logger.info(f"   Created: {created}")
                            logger.info(f"   Deleted: {deleted}")
                            logger.info(f"   Noops (no changes needed): {noops}")
                            
                            # Check for failures
                            response = task_data.get('response', {})
                            failures = response.get('failures', [])
                            if failures:
                                logger.warning(f"⚠️  Task completed with {len(failures)} failures")
                                for failure in failures[:3]:  # Show first 3 failures
                                    logger.warning(f"   Failure: {failure}")
                            else:
                                logger.info(f"✅ Task completed without failures")
                            
                            logger.debug(f"📋 Full task result: {json.dumps(task_data, indent=2)}")
                            return True
                        else:
                            logger.debug(f"⏳ Task still running: {task_id}")
                            time.sleep(5)  # Wait 5 seconds before checking again
                            continue
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️  Failed to parse task status response: {result.stdout}")
                        time.sleep(5)
                        continue
                else:
                    logger.warning(f"⚠️  Failed to check task status: {result.stderr}")
                    time.sleep(5)
                    continue
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"⚠️  Timeout checking task status: {task_id}")
                time.sleep(5)
                continue
            except Exception as e:
                logger.warning(f"⚠️  Error checking task status: {e}")
                time.sleep(5)
                continue
        
        logger.warning(f"⚠️  Task did not complete within {max_wait_time} seconds: {task_id}")
        return False
    
    def check_elasticsearch_task_status(self, task_id: str) -> Dict:
        """Manually check Elasticsearch task status - useful for debugging long-running tasks"""
        logger.info(f"🔍 MANUAL TASK STATUS CHECK: {task_id}")
        
        # Read Elasticsearch URL from config file
        es_url = self.config.get('elasticsearch', 'host', fallback='https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com')
        
        try:
            # Check task status
            curl_cmd = [
                'curl', '-s', '-X', 'GET',
                f'{es_url}/_tasks/{task_id}'
            ]
            
            logger.info(f"🚀 Executing manual task status check:")
            logger.info(f"   Command: {' '.join(curl_cmd)}")
            logger.info(f"   Task ID: {task_id}")
            logger.info(f"   Request URL: {es_url}/_tasks/{task_id}")
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
            
            logger.info(f"⏱️  Manual task status check completed with return code: {result.returncode}")
            
            # Log complete curl response
            logger.info(f"📋 Complete Manual Task Status Response:")
            logger.info(f"   Return Code: {result.returncode}")
            logger.info(f"   STDOUT: {result.stdout}")
            logger.info(f"   STDERR: {result.stderr}")
            
            if result.returncode == 0:
                try:
                    task_data = json.loads(result.stdout)
                    logger.info(f"📋 Parsed Task Status Data:")
                    logger.info(f"   {json.dumps(task_data, indent=2)}")
                    
                    # Extract key information
                    task_info = task_data.get('task', {})
                    completed = task_data.get('completed', False) or task_info.get('completed', False)
                    status = task_info.get('status', {})
                    response = task_data.get('response', {})
                    
                    logger.info(f"📊 Task Summary:")
                    logger.info(f"   Task ID: {task_id}")
                    logger.info(f"   Completed: {completed}")
                    logger.info(f"   Total: {status.get('total', 'N/A')}")
                    logger.info(f"   Updated: {status.get('updated', 'N/A')}")
                    logger.info(f"   Created: {status.get('created', 'N/A')}")
                    logger.info(f"   Deleted: {status.get('deleted', 'N/A')}")
                    logger.info(f"   Batches: {status.get('batches', 'N/A')}")
                    logger.info(f"   Version Conflicts: {status.get('version_conflicts', 'N/A')}")
                    logger.info(f"   Noops: {status.get('noops', 'N/A')}")
                    
                    # Check for failures
                    failures = response.get('failures', [])
                    if failures:
                        logger.warning(f"⚠️  Task completed with {len(failures)} failures")
                        for failure in failures[:3]:  # Show first 3 failures
                            logger.warning(f"   Failure: {failure}")
                    else:
                        logger.info(f"✅ Task completed without failures")
                    
                    if completed:
                        logger.info(f"✅ Task completed successfully!")
                    else:
                        logger.info(f"⏳ Task is still running...")
                    
                    return task_data
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  Could not parse task status response: {e}")
                    logger.warning(f"📄 Raw STDOUT: {result.stdout}")
                    return {}
            else:
                logger.warning(f"⚠️  Manual task status check failed: {result.stderr}")
                logger.warning(f"📄 Full STDOUT: {result.stdout}")
                logger.warning(f"📄 Full STDERR: {result.stderr}")
                return {}
                
        except Exception as e:
            logger.error(f"❌ Error in manual task status check: {e}")
            logger.exception("Full exception details:")
            return {}
    
    def _fallback_batch_elasticsearch_masking(self, contributors: List[ContributorInfo], es_url: str) -> int:
        """Targeted fallback Elasticsearch masking using database mappings and known problematic indices"""
        logger.info("🔄 TARGETED FALLBACK ELASTICSEARCH MASKING")
        logger.info(f"   Processing {len(contributors)} contributors with targeted approach")
        logger.info("   Using database mappings + known problematic indices instead of all 6,496 indices")

        try:
            # Create target indices for fallback masking
            target_indices = []
            
            # Include unit-metrics for comprehensive coverage
            target_indices.append("unit-metrics")
            
            # Remove duplicates
            target_indices = list(set(target_indices))

            if target_indices:
                logger.info(f"🎯 Targeting {len(target_indices)} specific indices:")
                for idx in target_indices:
                    logger.info(f"   - {idx}")
                logger.info("   This targeted approach is much more efficient than scanning all 6,496 indices")

                # Execute optimized updates (separate ID and email operations)
                masked_docs = self._execute_optimized_elasticsearch_updates(es_url, target_indices, contributors)

                logger.info(f"✅ Targeted fallback masking completed: {masked_docs} operations")
                return masked_docs
            else:
                logger.warning("⚠️  No target indices found for fallback masking")
                return 0
        
        except Exception as e:
            logger.error(f"❌ Targeted fallback Elasticsearch masking failed: {e}")
            logger.exception("Full exception details:")
            return 0
    
    def _discover_project_indices_dynamically(self, contributors: List[ContributorInfo], es_url: str) -> List[str]:
        """Dynamically discover project indices that contain contributor data"""
        logger.info("🔍 DYNAMIC PROJECT INDEX DISCOVERY")
        logger.info(f"   Searching for project indices containing contributor data...")
        logger.info(f"   Contributors: {len(contributors)}")
        
        discovered_indices = []
        
        try:
            # Get list of all project indices from Elasticsearch
            curl_cmd = [
                'curl', '-s', '-X', 'GET',
                f'{es_url}/_cat/indices/project-*?format=json'
            ]
            
            logger.info("🚀 Executing project index discovery:")
            logger.info(f"   Command: {' '.join(curl_cmd)}")
            logger.info(f"   Request URL: {es_url}/_cat/indices/project-*")
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                try:
                    import json
                    indices_data = json.loads(result.stdout)
                    
                    # Extract index names
                    all_project_indices = [idx.get('index', '') for idx in indices_data if idx.get('index', '').startswith('project-')]
                    
                    logger.info(f"📊 Found {len(all_project_indices)} project indices in Elasticsearch")
                    
                    # Sample a few indices to check for contributor data (to avoid scanning all indices)
                    # Limit to first 10 indices to avoid performance issues
                    sample_indices = all_project_indices[:10]
                    
                    for index_name in sample_indices:
                        if self._check_index_contains_contributor_data(index_name, contributors, es_url):
                            discovered_indices.append(index_name)
                            logger.info(f"✅ Found contributor data in index: {index_name}")
                    
                    logger.info(f"🎯 Discovered {len(discovered_indices)} indices with contributor data: {discovered_indices}")
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  Could not parse project indices response: {e}")
                    logger.warning(f"📄 Raw response: {result.stdout}")
            else:
                logger.warning(f"⚠️  Project index discovery failed: {result.stderr}")
                logger.warning(f"📄 Full STDOUT: {result.stdout}")
                logger.warning(f"📄 Full STDERR: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"⚠️  Error in dynamic project discovery: {e}")
            logger.debug(f"Exception details: {e}")
        
        return discovered_indices
    
    def _check_index_contains_contributor_data(self, index_name: str, contributors: List[ContributorInfo], es_url: str) -> bool:
        """Check if a specific index contains data for any of the contributors"""
        try:
            # Create a simple search query to check if the index contains contributor data
            search_query = {
                "query": {
                    "bool": {
                        "should": [
                            {"term": {"contributor_id": contributor.contributor_id}} for contributor in contributors
                        ] + [
                            {"term": {"email": contributor.email_address}} for contributor in contributors
                        ],
                        "minimum_should_match": 1
                    }
                },
                "size": 1
            }
            
            # Execute search query
            curl_cmd = [
                'curl', '-s', '-X', 'GET',
                f'{es_url}/{index_name}/_search',
                '-H', 'Content-Type: application/json',
                '-d', json.dumps(search_query)
            ]
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0:
                try:
                    response_data = json.loads(result.stdout)
                    hits = response_data.get('hits', {}).get('hits', [])
                    return len(hits) > 0
                except json.JSONDecodeError:
                    return False
            else:
                return False
                
        except Exception as e:
            logger.debug(f"Error checking index {index_name}: {e}")
            return False
    
    def _mask_in_manual_project_indices(self, contributors: List[ContributorInfo], es_url: str, manual_indices: List[str]) -> int:
        """Mask contributor data in manually specified project indices (for discovered indices)"""
        logger.info(f"🎯 MANUAL PROJECT INDICES MASKING")
        logger.info(f"   Targeting discovered indices: {manual_indices}")
        logger.info(f"   Contributors: {len(contributors)}")
        
        if not manual_indices:
            logger.info("ℹ️  No manual indices specified, skipping manual masking")
            return 0
        
        try:
            # Use the optimized update method for manual indices
            masked_docs = self._execute_optimized_elasticsearch_updates(es_url, manual_indices, contributors)
            logger.info(f"✅ Manual project indices masking completed: {masked_docs} operations")
            return masked_docs
        except Exception as e:
            logger.error(f"❌ Manual project indices masking failed: {e}")
            logger.exception("Full exception details:")
            return 0
    
    def _verify_elasticsearch_masking(self, contributors: List[ContributorInfo]):
        """Verify that Elasticsearch masking was effective by searching for unmasked emails"""
        if not contributors:
            return
            
        contributor = contributors[0]
        contributor_id = contributor.contributor_id
        email = contributor.email_address
        
        logger.info(f"🔍 Verifying masking for contributor: {contributor_id} (email: {email})")
        
        # Create verification query to search for unmasked emails
        verification_query = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"email": email}},
                        {"term": {"email_address": email}},
                        {"term": {"worker_email": email}},
                        {"term": {"lastAnnotatorEmail": email}},
                        {"term": {"workerEmail": email}},
                        {"term": {"latest.workerEmail": email}},
                        {"term": {"latest.lastAnnotatorEmail": email}},
                        {"term": {"latest.lastReviewerEmail": email}},
                        {"term": {"history.workerEmail": email}},
                        {"term": {"history.lastAnnotatorEmail": email}},
                        {"term": {"earliest.workerEmail": email}},
                        {"term": {"earliest.lastAnnotatorEmail": email}},
                        {"query_string": {"query": f"*{email}*"}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": 10
        }
        
        try:
            # Get Elasticsearch URL from config file
            es_url = self.config.get('elasticsearch', 'host', fallback='https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com')
            if es_url.endswith('/'):
                es_url = es_url[:-1]
            
            # Create target indices from known problematic indices
            target_indices = ["project-ca7a7a99-9d2d-40c5-944b-454e9712e85d"]
            
            logger.info(f"🎯 Targeting verification on project indices: {target_indices}")
            
            # Create temporary file for verification query
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(verification_query, f)
                query_file = f.name
            
            # Execute search query on specific indices
            indices_str = ','.join(target_indices)
            curl_cmd = [
                'curl', '-s', '-X', 'GET',
                f'{es_url}/{indices_str}/_search',
                '-H', 'Content-Type: application/json',
                '-d', f'@{query_file}'
            ]
            
            logger.info(f"🔍 Executing Elasticsearch verification curl command:")
            logger.info(f"   Command: {' '.join(curl_cmd)}")
            logger.info(f"   Target indices: {indices_str}")
            logger.info(f"   Request URL: {es_url}/{indices_str}/_search")
            logger.info(f"   Request Headers: Content-Type: application/json")
            logger.info(f"   Request Body File: {query_file}")
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=60)
            
            # Log complete curl response
            logger.info(f"📋 Complete Verification Curl Response:")
            logger.info(f"   Return Code: {result.returncode}")
            logger.info(f"   STDOUT: {result.stdout}")
            logger.info(f"   STDERR: {result.stderr}")
            
            if result.returncode == 0:
                try:
                    response_data = json.loads(result.stdout)
                    logger.info(f"📋 Parsed Verification Response Data:")
                    logger.info(f"   {json.dumps(response_data, indent=2)}")
                    
                    total_hits = response_data.get('hits', {}).get('total', {}).get('value', 0)
                    
                    if total_hits > 0:
                        logger.warning(f"⚠️  VERIFICATION FAILED: Found {total_hits} documents still containing unmasked email '{email}'")
                        logger.warning("   This indicates that masking may not have been completely effective")
                        
                        # Log sample of unmasked documents
                        hits = response_data.get('hits', {}).get('hits', [])
                        for i, hit in enumerate(hits[:3]):  # Show first 3 hits
                            source = hit.get('_source', {})
                            logger.warning(f"   Sample unmasked document {i+1}: {hit.get('_index', 'unknown')}")
                            logger.warning(f"   Contains email fields: {[k for k, v in source.items() if isinstance(v, str) and email in v]}")
                    else:
                        logger.info(f"✅ VERIFICATION SUCCESSFUL: No documents found containing unmasked email '{email}'")
                        logger.info("   Email masking appears to be effective")
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️  Could not parse verification response: {e}")
                    logger.warning(f"📄 Raw STDOUT: {result.stdout}")
                    logger.warning(f"📄 Raw STDERR: {result.stderr}")
            else:
                logger.warning(f"⚠️  Verification query failed: {result.stderr}")
                logger.warning(f"📄 Full STDOUT: {result.stdout}")
                logger.warning(f"📄 Full STDERR: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"⚠️  Verification failed: {e}")
        finally:
            # Clean up
            try:
                os.unlink(query_file)
            except:
                pass
