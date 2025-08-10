#!/usr/bin/env python3
"""
ETL Data Connector for NewsAPI
Author: Shankari S R - 3122225001125
Description: Custom ETL pipeline to extract news data from NewsAPI,
transform it for MongoDB compatibility, and load it into MongoDB collection.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from pymongo.collection import Collection
import urllib3

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_connector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETLConnector:
    """Custom ETL Data Connector for NewsAPI data ingestion into MongoDB"""
    
    def __init__(self):
        """Initialize the ETL connector with configuration"""
        # NewsAPI specific configuration
        self.api_base_url = os.getenv('API_BASE_URL', 'https://newsapi.org/v2/top-headlines')
        self.api_key = os.getenv('API_KEY', '').strip('"')  # Remove quotes if present
        self.mongo_uri = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.mongo_db = os.getenv('MONGO_DB', 'etl_database')
        self.collection_name = os.getenv('COLLECTION_NAME', 'newsapi')
        
        # Rate limiting
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '1.0'))
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        
        # MongoDB client
        self.mongo_client = None
        self.db = None
        self.collection = None
        
        # Validate API key
        if not self.api_key:
            logger.error("API_KEY is required for NewsAPI")
            raise ValueError("API_KEY is required for NewsAPI")
        
        logger.info("NewsAPI ETL Connector initialized")
    
    def connect_mongodb(self) -> bool:
        """Establish connection to MongoDB"""
        try:
            self.mongo_client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            # Test connection
            self.mongo_client.admin.command('ping')
            self.db = self.mongo_client[self.mongo_db]
            self.collection = self.db[self.collection_name]
            logger.info(f"Connected to MongoDB: {self.mongo_db}.{self.collection_name}")
            return True
        except errors.ServerSelectionTimeoutError as e:
            logger.error(f"MongoDB connection failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected MongoDB error: {e}")
            return False
    
    def disconnect_mongodb(self):
        """Close MongoDB connection"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
    
    def make_api_request(self, params: Optional[Dict] = None) -> Optional[Dict]:
        """Make API request to NewsAPI with error handling and retries"""
        url = self.api_base_url
        headers = {
            'User-Agent': 'ETL-Connector/1.0',
            'Accept': 'application/json',
            'X-Api-Key': self.api_key  # NewsAPI uses X-Api-Key header
        }
        
        # Default parameters for NewsAPI
        default_params = {
            'country': 'us',  # You can change this to your preferred country
            'pageSize': 100   # Maximum articles per request
        }
        
        if params:
            default_params.update(params)
        
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Making request to: {url} (Attempt {attempt + 1})")
                
                response = requests.get(
                    url,
                    headers=headers,
                    params=default_params,
                    timeout=30,
                    verify=True  # NewsAPI supports SSL
                )
                
                # Rate limiting
                time.sleep(self.rate_limit_delay)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 401:
                    logger.error("Invalid API key")
                    return None
                elif response.status_code == 426:
                    logger.error("API key requires upgrade")
                    return None
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    
            except requests.exceptions.Timeout:
                logger.error(f"Request timeout (attempt {attempt + 1})")
            except requests.exceptions.ConnectionError:
                logger.error(f"Connection error (attempt {attempt + 1})")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e} (attempt {attempt + 1})")
            except json.JSONDecodeError:
                logger.error("Invalid JSON response")
            
            if attempt < self.max_retries - 1:
                wait_time = (2 ** attempt) * 2  # Exponential backoff
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
        
        return None
    
    def extract_data(self) -> List[Dict]:
        """Extract data from NewsAPI"""
        all_articles = []
        
        # Extract top headlines
        response_data = self.make_api_request()
        
        if response_data and response_data.get('status') == 'ok':
            articles = response_data.get('articles', [])
            logger.info(f"Extracted {len(articles)} articles")
            all_articles.extend(articles)
            
            # Log API usage info
            total_results = response_data.get('totalResults', len(articles))
            logger.info(f"Total results available: {total_results}")
            
        else:
            if response_data:
                logger.error(f"API Error: {response_data.get('message', 'Unknown error')}")
            else:
                logger.error("No data received from API")
        
        logger.info(f"Total extracted articles: {len(all_articles)}")
        return all_articles
    
    def transform_data(self, raw_data: List[Dict]) -> List[Dict]:
        """Transform data for MongoDB compatibility"""
        transformed_data = []
        current_timestamp = datetime.utcnow()
        
        for idx, article in enumerate(raw_data):
            try:
                # Create unique ID based on URL or title
                source_id = article.get('url', f"article_{idx}")
                article_id = f"news_{hash(source_id)}_{int(current_timestamp.timestamp())}"
                
                # Create transformed record
                transformed_record = {
                    '_id': article_id,
                    'title': (article.get('title') or '').strip(),
                    'description': (article.get('description') or '').strip(),
                    'content': (article.get('content') or '').strip(),
                    'url': article.get('url') or '',
                    'url_to_image': article.get('urlToImage') or '',
                    'published_at': article.get('publishedAt') or '',
                    'author': article.get('author') or '',
                    
                    # Source information
                    'source': {
                        'id': article.get('source', {}).get('id', ''),
                        'name': article.get('source', {}).get('name', '')
                    },
                    
                    # Data quality fields
                    'data_quality': {
                        'title_length': len(article.get('title', '')),
                        'description_length': len(article.get('description', '')),
                        'content_length': len(article.get('content', '')),
                        'has_image': bool(article.get('urlToImage')),
                        'has_author': bool(article.get('author')),
                        'has_description': bool(article.get('description'))
                    },
                    
                    # ETL metadata
                    'etl_metadata': {
                        'ingestion_timestamp': current_timestamp,
                        'ingestion_date': current_timestamp.strftime('%Y-%m-%d'),
                        'source': 'newsapi',
                        'connector_version': '1.0',
                        'data_type': 'news_article'
                    }
                }
                
                # Data validation
                issues = []
                if not transformed_record['title']:
                    issues.append('missing_title')
                if not transformed_record['description']:
                    issues.append('missing_description')
                if not transformed_record['url']:
                    issues.append('missing_url')
                
                if issues:
                    logger.warning(f"Article {idx} has issues: {issues}")
                    transformed_record['etl_metadata']['data_quality_issues'] = issues
                
                # Parse published date
                try:
                    if transformed_record['published_at']:
                        published_dt = datetime.fromisoformat(
                            transformed_record['published_at'].replace('Z', '+00:00')
                        )
                        transformed_record['published_date'] = published_dt.strftime('%Y-%m-%d')
                        transformed_record['published_timestamp'] = published_dt
                except Exception as e:
                    logger.warning(f"Could not parse published date: {e}")
                    transformed_record['published_date'] = None
                    transformed_record['published_timestamp'] = None
                
                transformed_data.append(transformed_record)
                
            except Exception as e:
                logger.error(f"Error transforming article {idx}: {e}")
                continue
        
        logger.info(f"Transformed {len(transformed_data)} articles")
        return transformed_data
    
    def load_data(self, transformed_data: List[Dict]) -> bool:
        """Load transformed data into MongoDB"""
        if not transformed_data:
            logger.warning("No data to load")
            return False
        
        try:
            # Create indexes for better performance (if they don't exist)
            try:
                self.collection.create_index([
                    ('etl_metadata.ingestion_date', 1),
                    ('published_date', -1)
                ], background=True)
            except errors.OperationFailure:
                pass  # Index might already exist
            
            try:
                self.collection.create_index([('url', 1)], sparse=True, background=True)
            except errors.OperationFailure:
                pass  # Index might already exist
            
            try:
                self.collection.create_index([('source.name', 1)], background=True)
            except errors.OperationFailure:
                pass  # Index might already exist
            
            # Handle duplicates - update existing or insert new
            successful_operations = 0
            
            for article in transformed_data:
                try:
                    # Check if article already exists by URL
                    if article['url']:
                        existing = self.collection.find_one({'url': article['url']})
                        if existing:
                            # Update existing document (exclude _id from update)
                            update_data = {k: v for k, v in article.items() if k != '_id'}
                            result = self.collection.update_one(
                                {'url': article['url']},
                                {'$set': update_data}
                            )
                            if result.modified_count > 0:
                                successful_operations += 1
                                logger.info(f"Updated existing article: {article['title'][:50]}...")
                        else:
                            # Insert new document
                            self.collection.insert_one(article)
                            successful_operations += 1
                            logger.info(f"Inserted new article: {article['title'][:50]}...")
                    else:
                        # No URL available, insert as new (assuming it's unique)
                        try:
                            self.collection.insert_one(article)
                            successful_operations += 1
                            logger.info(f"Inserted article without URL: {article['title'][:50]}...")
                        except errors.DuplicateKeyError:
                            # Skip duplicate
                            logger.warning(f"Skipped duplicate article: {article['title'][:50]}...")
                            
                except Exception as e:
                    logger.error(f"Error processing article: {e}")
                    continue
            
            logger.info(f"Successfully processed {successful_operations} articles")
            
            # Log summary statistics
            self.log_load_summary(successful_operations)
            
            return successful_operations > 0
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def log_load_summary(self, records_loaded: int):
        """Log summary of the ETL process"""
        try:
            # Get collection stats
            total_records = self.collection.count_documents({})
            today_records = self.collection.count_documents({
                'etl_metadata.ingestion_date': datetime.utcnow().strftime('%Y-%m-%d')
            })
            
            # Get source distribution
            source_pipeline = [
                {'$group': {'_id': '$source.name', 'count': {'$sum': 1}}},
                {'$sort': {'count': -1}},
                {'$limit': 5}
            ]
            top_sources = list(self.collection.aggregate(source_pipeline))
            
            logger.info(f"""
            ETL Process Summary:
            - Records processed this run: {records_loaded}
            - Records ingested today: {today_records}
            - Total records in collection: {total_records}
            - Collection: {self.collection_name}
            - Top sources: {[f"{s['_id']}: {s['count']}" for s in top_sources]}
            """)
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
    
    def run_etl_pipeline(self) -> bool:
        """Run the complete ETL pipeline"""
        logger.info("Starting NewsAPI ETL pipeline...")
        
        try:
            # Connect to MongoDB
            if not self.connect_mongodb():
                return False
            
            # Extract
            logger.info("Phase 1: Extracting data from NewsAPI...")
            raw_data = self.extract_data()
            if not raw_data:
                logger.error("No data extracted")
                return False
            
            # Transform
            logger.info("Phase 2: Transforming data...")
            transformed_data = self.transform_data(raw_data)
            if not transformed_data:
                logger.error("No data transformed")
                return False
            
            # Load
            logger.info("Phase 3: Loading data...")
            success = self.load_data(transformed_data)
            
            if success:
                logger.info("ETL pipeline completed successfully!")
                return True
            else:
                logger.error("ETL pipeline failed during load phase")
                return False
                
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return False
        finally:
            self.disconnect_mongodb()
    
    def validate_data(self) -> Dict[str, Any]:
        """Validate the loaded data quality"""
        if not self.connect_mongodb():
            return {}
        
        try:
            validation_results = {
                'total_records': self.collection.count_documents({}),
                'today_records': self.collection.count_documents({
                    'etl_metadata.ingestion_date': datetime.utcnow().strftime('%Y-%m-%d')
                }),
                'records_with_issues': self.collection.count_documents({
                    'etl_metadata.data_quality_issues': {'$exists': True}
                }),
                'records_with_images': self.collection.count_documents({
                    'url_to_image': {'$ne': '', '$exists': True}
                }),
                'unique_sources': len(self.collection.distinct('source.name'))
            }
            
            # Calculate averages
            pipeline = [
                {'$group': {
                    '_id': None,
                    'avg_title_len': {'$avg': '$data_quality.title_length'},
                    'avg_desc_len': {'$avg': '$data_quality.description_length'}
                }}
            ]
            
            result = list(self.collection.aggregate(pipeline))
            if result:
                validation_results['average_title_length'] = round(result[0].get('avg_title_len', 0), 2)
                validation_results['average_description_length'] = round(result[0].get('avg_desc_len', 0), 2)
            
            logger.info(f"Data validation results: {validation_results}")
            return validation_results
            
        except Exception as e:
            logger.error(f"Data validation error: {e}")
            return {}
        finally:
            self.disconnect_mongodb()


def main():
    """Main execution function"""
    logger.info("=" * 50)
    logger.info("NewsAPI ETL Data Connector Starting...")
    logger.info("=" * 50)
    
    # Initialize connector
    try:
        connector = ETLConnector()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        return
    
    # Run ETL pipeline
    success = connector.run_etl_pipeline()
    
    if success:
        # Validate data
        logger.info("Running data validation...")
        validation_results = connector.validate_data()
        
        if validation_results:
            print("\n" + "=" * 50)
            print("ETL PROCESS COMPLETED SUCCESSFULLY!")
            print("=" * 50)
            print(f"Total Records: {validation_results.get('total_records', 0)}")
            print(f"Today's Records: {validation_results.get('today_records', 0)}")
            print(f"Records with Issues: {validation_results.get('records_with_issues', 0)}")
            print(f"Records with Images: {validation_results.get('records_with_images', 0)}")
            print(f"Unique Sources: {validation_results.get('unique_sources', 0)}")
            print(f"Average Title Length: {validation_results.get('average_title_length', 0)}")
            print(f"Average Description Length: {validation_results.get('average_description_length', 0)}")
            print("=" * 50)
    else:
        print("\nETL PROCESS FAILED!")
        print("Check the logs for more details.")


if __name__ == "__main__":
    main()