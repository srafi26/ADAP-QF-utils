#!/usr/bin/env python3
"""
Manual Elasticsearch Masking Script

This script manually masks contributor data in Elasticsearch when the automatic
deletion process doesn't find the project IDs correctly.
"""

import requests
import json
import logging
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

def mask_contributor_in_elasticsearch(contributor_id: str, email: str, project_id: str, es_url: str):
    """Manually mask contributor data in Elasticsearch"""
    
    index_name = f"project-{project_id}"
    logger.info(f"🎯 Masking contributor {contributor_id} ({email}) in index: {index_name}")
    
    # Elasticsearch URL for integration
    if not es_url:
        es_url = "https://vpc-kepler-es-integration-v1-gsffeklbxeuvx3zx5t3qm3xht4.us-east-1.es.amazonaws.com"
    
    # Search for documents containing this contributor
    search_url = f"{es_url}/{index_name}/_search"
    
    # Query to find documents with this contributor
    search_query = {
        "query": {
            "bool": {
                "should": [
                    {"term": {"latest.workerId.keyword": contributor_id}},
                    {"term": {"latest.workerEmail.keyword": email}},
                    {"term": {"workerId.keyword": contributor_id}},
                    {"term": {"workerEmail.keyword": email}},
                    {"term": {"history.workerId.keyword": contributor_id}},
                    {"term": {"history.workerEmail.keyword": email}},
                    {"term": {"history.lastAnnotatorEmail.keyword": email}},
                    {"term": {"history.lastAnnotator.keyword": contributor_id}},
                    {"term": {"earliest.workerId.keyword": contributor_id}},
                    {"term": {"earliest.workerEmail.keyword": email}}
                ]
            }
        },
        "size": 10000
    }
    
    logger.info(f"🔍 Searching for documents with contributor {contributor_id} or email {email}")
    logger.debug(f"Search URL: {search_url}")
    logger.debug(f"Search query: {json.dumps(search_query, indent=2)}")
    
    try:
        # Search for documents
        response = requests.post(search_url, json=search_query, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            hits = data.get('hits', {}).get('hits', [])
            total_hits = data.get('hits', {}).get('total', {}).get('value', 0)
            
            logger.info(f"📊 Found {total_hits} documents to mask")
            
            if total_hits == 0:
                logger.warning(f"⚠️ No documents found for contributor {contributor_id}")
                return 0
            
            # Process each document
            masked_count = 0
            for hit in hits:
                doc_id = hit['_id']
                doc_source = hit['_source']
                
                logger.debug(f"Processing document {doc_id}")
                
                # Create update query to mask PII fields
                update_query = {
                    "script": {
                        "source": """
                            // Mask latest fields
                            if (ctx._source.latest != null) {
                                if (ctx._source.latest.workerId == params.contributor_id) {
                                    ctx._source.latest.workerId = 'DELETED_USER';
                                }
                                if (ctx._source.latest.workerEmail == params.email) {
                                    ctx._source.latest.workerEmail = 'deleted_user@deleted.com';
                                }
                            }
                            
                            // Mask direct fields
                            if (ctx._source.workerId == params.contributor_id) {
                                ctx._source.workerId = 'DELETED_USER';
                            }
                            if (ctx._source.workerEmail == params.email) {
                                ctx._source.workerEmail = 'deleted_user@deleted.com';
                            }
                            
                            // Mask history array (history is an array of objects)
                            if (ctx._source.history != null) {
                                if (ctx._source.history instanceof List) {
                                    // History is an array - create new array with masked values
                                    def newHistory = [];
                                    for (int i = 0; i < ctx._source.history.size(); i++) {
                                        def historyEntry = ctx._source.history[i];
                                        def newEntry = [:];
                                        // Copy all fields from original entry
                                        for (def key : historyEntry.keySet()) {
                                            newEntry[key] = historyEntry[key];
                                        }
                                        // Mask specific fields
                                        if (historyEntry.workerId == params.contributor_id) {
                                            newEntry.workerId = 'DELETED_USER';
                                        }
                                        if (historyEntry.workerEmail == params.email) {
                                            newEntry.workerEmail = 'deleted_user@deleted.com';
                                        }
                                        if (historyEntry.lastAnnotatorEmail == params.email) {
                                            newEntry.lastAnnotatorEmail = 'deleted_user@deleted.com';
                                        }
                                        if (historyEntry.lastAnnotator == params.contributor_id) {
                                            newEntry.lastAnnotator = 'DELETED_USER';
                                        }
                                        newHistory.add(newEntry);
                                    }
                                    ctx._source.history = newHistory;
                                } else {
                                    // History is a single object (fallback)
                                    if (ctx._source.history.workerId == params.contributor_id) {
                                        ctx._source.history.workerId = 'DELETED_USER';
                                    }
                                    if (ctx._source.history.workerEmail == params.email) {
                                        ctx._source.history.workerEmail = 'deleted_user@deleted.com';
                                    }
                                    if (ctx._source.history.lastAnnotatorEmail == params.email) {
                                        ctx._source.history.lastAnnotatorEmail = 'deleted_user@deleted.com';
                                    }
                                    if (ctx._source.history.lastAnnotator == params.contributor_id) {
                                        ctx._source.history.lastAnnotator = 'DELETED_USER';
                                    }
                                }
                            }
                            
                            // Mask earliest fields
                            if (ctx._source.earliest != null) {
                                if (ctx._source.earliest.workerId == params.contributor_id) {
                                    ctx._source.earliest.workerId = 'DELETED_USER';
                                }
                                if (ctx._source.earliest.workerEmail == params.email) {
                                    ctx._source.earliest.workerEmail = 'deleted_user@deleted.com';
                                }
                            }
                        """,
                        "params": {
                            "contributor_id": contributor_id,
                            "email": email
                        }
                    }
                }
                
                # Update the document
                update_url = f"{es_url}/{index_name}/_update/{doc_id}"
                update_response = requests.post(update_url, json=update_query, timeout=30)
                
                if update_response.status_code == 200:
                    masked_count += 1
                    logger.debug(f"✅ Masked document {doc_id}")
                else:
                    logger.error(f"❌ Failed to mask document {doc_id}: {update_response.text}")
            
            logger.info(f"✅ Successfully masked {masked_count}/{total_hits} documents")
            return masked_count
            
        else:
            logger.error(f"❌ Search failed: {response.status_code} - {response.text}")
            return 0
            
    except Exception as e:
        logger.error(f"❌ Error masking contributor data: {e}")
        return 0

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manually mask contributor data in Elasticsearch')
    parser.add_argument('--contributor-id', required=True, help='Contributor ID to mask')
    parser.add_argument('--email', required=True, help='Contributor email to mask')
    parser.add_argument('--project-id', required=True, help='Project ID containing the data')
    parser.add_argument('--es-url', help='Elasticsearch URL (defaults to integration)')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting manual Elasticsearch masking")
    logger.info(f"Contributor ID: {args.contributor_id}")
    logger.info(f"Email: {args.email}")
    logger.info(f"Project ID: {args.project_id}")
    
    masked_count = mask_contributor_in_elasticsearch(
        args.contributor_id,
        args.email,
        args.project_id,
        args.es_url
    )
    
    logger.info(f"🎉 Manual masking completed. {masked_count} documents masked.")

if __name__ == "__main__":
    main()
