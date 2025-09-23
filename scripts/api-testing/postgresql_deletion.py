#!/usr/bin/env python3
"""
PostgreSQL deletion operations for contributor deletion system
"""

import logging
import time
from typing import List
from contributor_deletion_base import ContributorInfo

logger = logging.getLogger(__name__)

class PostgreSQLDeletion:
    """Handles PostgreSQL data deletion operations"""
    
    def __init__(self, config, integration: bool = False, dry_run: bool = True):
        self.config = config
        self.integration = integration
        self.dry_run = dry_run
        
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
    
    def delete_postgresql_data(self, contributors: List[ContributorInfo], db_connections) -> int:
        """Delete contributor data from PostgreSQL"""
        if self.dry_run:
            logger.info("DRY RUN: Would delete PostgreSQL data")
            return len(contributors)
        
        logger.info("Connecting to PostgreSQL for data deletion...")
        logger.debug(f"PostgreSQL connection config: host={self.config.get('database', 'host', fallback='localhost')}, port={self.config.get('database', 'port', fallback='5432')}, database={self.config.get('database', 'database', fallback='kepler-app-db')}")
        deleted_records = 0
        
        try:
            # Get a fresh PostgreSQL connection for this operation
            conn = db_connections.get_postgres_connection(force_fresh=True)
            cursor = conn.cursor()
            logger.info("✅ Fresh PostgreSQL connection established for deactivation")
        except Exception as e:
            logger.warning(f"PostgreSQL connection failed (this may be expected for integration environment): {e}")
            logger.info("Skipping PostgreSQL data deletion and continuing with other operations...")
            return 0
        
        try:
            # Extract contributor IDs and email addresses
            contributor_ids = [c.contributor_id for c in contributors]
            email_addresses = [c.email_address for c in contributors]
            
            contributor_ids_str = "', '".join(contributor_ids)
            logger.info(f"Processing {len(contributor_ids)} contributor IDs for PostgreSQL deletion")
            logger.debug(f"Contributor IDs to process: {contributor_ids}")
            
            # First, check if any of these contributors actually exist in the database
            logger.info("Checking if contributors exist in the database...")
            check_query = f"SELECT COUNT(*) FROM kepler_crowd_contributors_t WHERE id IN ('{contributor_ids_str}')"
            cursor.execute(check_query)
            existing_count = cursor.fetchone()[0]
            logger.info(f"Found {existing_count} existing contributors out of {len(contributor_ids)} requested")
            
            if existing_count == 0:
                logger.warning("No contributors found in the database - they may have already been deleted")
                logger.info("Skipping PostgreSQL deletion as no contributors exist")
                return 0
            
            # Deactivate records in tables using contributor_id
            logger.info(f"Processing {len(self.contributor_id_tables)} contributor_id tables...")
            logger.debug(f"Tables to process: {self.contributor_id_tables}")
            
            # Process each table with proper transaction management
            # Process tables with status column first (critical for access control)
            for table in self.tables_with_status:
                try:
                    logger.debug(f"Processing table with status column: {table}")
                    
                    if table == 'kepler_crowd_contributors_t':
                        # For main table, deactivate and mask PII data
                        logger.debug(f"Deactivating and masking PII data in main table: {table}")
                        self._deactivate_and_mask_pii_data(cursor, table, contributor_ids_str)
                    else:
                        logger.debug(f"Deactivating records in table: {table}")
                        query = f"UPDATE {table} SET status = 'INACTIVE' WHERE contributor_id IN ('{contributor_ids_str}')"
                        logger.debug(f"Executing query: {query}")
                        cursor.execute(query)
                        deleted_records += cursor.rowcount
                        if cursor.rowcount > 0:
                            logger.info(f"Deactivated {cursor.rowcount} records in {table}")
                        else:
                            logger.debug(f"No records found to deactivate in {table}")
                    
                    logger.debug(f"Successfully processed table: {table}")
                
                except Exception as e:
                    logger.warning(f"Error processing table {table}: {e}")
                    logger.debug(f"Exception details: {e}")
                    # Check if transaction is still valid
                    try:
                        # Test if transaction is still active
                        cursor.execute("SELECT 1")
                        logger.debug(f"Transaction is still active, continuing with next table")
                    except Exception as tx_error:
                        logger.error(f"Transaction is aborted: {tx_error}")
                        logger.error(f"Rolling back entire transaction and reconnecting...")
                        # Rollback the entire transaction
                        conn.rollback()
                        # Re-establish connection
                        conn = db_connections.get_postgres_connection()
                        cursor = conn.cursor()
                        logger.info(f"Transaction rolled back and connection re-established")
                    # Continue with next table
                    continue
            
            # Process tables without status column (audit logging only)
            logger.info("Processing tables without status column (audit logging only)...")
            for table in self.tables_without_status:
                try:
                    logger.debug(f"Processing table without status column: {table}")
                    
                    # For tables without status column, just count records for audit purposes
                    count_query = f"SELECT COUNT(*) FROM {table} WHERE contributor_id IN ('{contributor_ids_str}')"
                    logger.debug(f"Executing count query: {count_query}")
                    cursor.execute(count_query)
                    record_count = cursor.fetchone()[0]
                    
                    if record_count > 0:
                        logger.info(f"📊 Found {record_count} records in {table} (no status column - audit only)")
                        logger.info(f"   Note: {table} does not have a status column, so records cannot be deactivated")
                        logger.info(f"   These records will remain in the database for audit purposes")
                    else:
                        logger.debug(f"No records found in {table}")
                    
                    logger.debug(f"Successfully processed table: {table}")
                
                except Exception as e:
                    logger.warning(f"Error processing table {table}: {e}")
                    logger.debug(f"Exception details: {e}")
                    # Continue with next table
                    continue
            
            # Handle special tables with different column names
            logger.info("Processing special tables with different column names...")
            
            # Handle kepler_crowd_contributor_mercury_mapping_t (uses contributor_project_id)
            try:
                logger.debug("Processing kepler_crowd_contributor_mercury_mapping_t (uses contributor_project_id)")
                # This table uses contributor_project_id, not contributor_id
                # We need to find the contributor_project_id values first
                # Get email addresses for the contributors to match against
                email_addresses_str = "', '".join(email_addresses)
                mercury_query = """
                    DELETE FROM kepler_crowd_contributor_mercury_mapping_t 
                    WHERE contributor_project_id IN (
                        SELECT id FROM kepler_crowd_contributors_t 
                        WHERE email_address IN ('{email_addresses_str}')
                    )
                """.format(email_addresses_str=email_addresses_str)
                logger.debug(f"Executing mercury mapping query: {mercury_query}")
                cursor.execute(mercury_query)
                mercury_deleted = cursor.rowcount
                deleted_records += mercury_deleted
                if mercury_deleted > 0:
                    logger.info(f"Deleted {mercury_deleted} records from kepler_crowd_contributor_mercury_mapping_t")
                else:
                    logger.debug("No records found to delete in kepler_crowd_contributor_mercury_mapping_t")
                
                logger.debug(f"Successfully processed kepler_crowd_contributor_mercury_mapping_t")
                
            except Exception as e:
                logger.warning(f"Error processing kepler_crowd_contributor_mercury_mapping_t: {e}")
                logger.debug(f"Exception details: {e}")
                # Check if transaction is still valid
                try:
                    # Test if transaction is still active
                    cursor.execute("SELECT 1")
                    logger.debug(f"Transaction is still active, continuing")
                except Exception as tx_error:
                    logger.error(f"Transaction is aborted: {tx_error}")
                    logger.error(f"Rolling back entire transaction and reconnecting...")
                    # Rollback the entire transaction
                    conn.rollback()
                    # Re-establish connection
                    conn = db_connections.get_postgres_connection()
                    cursor = conn.cursor()
                    logger.info(f"Transaction rolled back and connection re-established")
                # Continue with other operations
            
            logger.info("Committing PostgreSQL transaction...")
            conn.commit()
            logger.info("PostgreSQL transaction committed successfully")
        
        except Exception as e:
            logger.error(f"Error deleting PostgreSQL data: {e}")
            logger.error("Rolling back PostgreSQL transaction...")
            conn.rollback()
            raise
        
        logger.info(f"PostgreSQL data deletion completed: {deleted_records} total records deleted/masked")
        return deleted_records
    
    def _deactivate_and_mask_pii_data(self, cursor, table: str, contributor_ids_str: str):
        """Deactivate and mask PII data instead of deleting from main contributor table"""
        try:
            logger.info(f"🚫 DEACTIVATING AND MASKING PII DATA in table: {table}")
            logger.debug(f"Executing deactivation and PII masking query for table: {table}")
            
            # Update contributor data to deactivate and mask PII
            if table == 'kepler_crowd_contributors_t':
                where_clause = f"WHERE id IN ('{contributor_ids_str}')"
                logger.info(f"🔑 Using 'id' column for main contributors table: {table}")
            else:
                where_clause = f"WHERE contributor_id IN ('{contributor_ids_str}')"
                logger.info(f"🔑 Using 'contributor_id' column for mapping table: {table}")
            
            # Create unique email for each contributor to avoid conflicts
            unique_email = f"deleted_user_{table}_{contributor_ids_str.replace(',', '_').replace(chr(39), '')}@deleted.com"
            
            query = f"""
                UPDATE {table} 
                SET 
                    status = 'INACTIVE',
                    name = 'DELETED_USER',
                    email_address = '{unique_email}',
                    country = 'DELETED',
                    age = 'DELETED',
                    gender = 'DELETED',
                    mobile_os = 'DELETED',
                    ethnicity = 'DELETED',
                    language = 'DELETED',
                    updated_at = NOW(),
                    updated_by = 'contributor_deletion_script'
                {where_clause}
            """
            
            logger.debug(f"Executing deactivation and PII masking query: {query}")
            
            start_time = time.time()
            cursor.execute(query)
            elapsed_time = time.time() - start_time
            
            deactivated_records = cursor.rowcount
            logger.info(f"✅ PostgreSQL deactivation and PII masking completed:")
            logger.info(f"   📊 Records deactivated and masked: {deactivated_records}")
            logger.info(f"   ⏱️  Execution time: {elapsed_time:.2f}s")
            logger.info(f"   🎯 Table: {table}")
            logger.debug(f"Deactivation and PII masking completed for table {table} with {deactivated_records} records affected")
        
        except Exception as e:
            logger.error(f"❌ Error deactivating and masking PII data in {table}: {e}")
            logger.error(f"   Failed query: {query}")
            logger.exception("Full exception details:")
            raise
