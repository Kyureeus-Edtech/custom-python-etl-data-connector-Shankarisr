#!/usr/bin/env python3
"""
Test script for NewsAPI ETL Connector
"""

import sys
import os
import logging
from datetime import datetime

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from etl_connector import ETLConnector  # Updated import
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure the etl_connector.py file is in the same directory")
    sys.exit(1)

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_mongodb_connection():
    """Test MongoDB connection"""
    print("Testing MongoDB connection...")
    try:
        connector = ETLConnector()
        if connector.connect_mongodb():
            print("✓ MongoDB connection successful")
            connector.disconnect_mongodb()
            return True
        else:
            print("✗ MongoDB connection failed")
            return False
    except Exception as e:
        print(f"✗ MongoDB connection error: {e}")
        return False


def test_api_connection():
    """Test NewsAPI connection"""
    print("Testing NewsAPI connection...")
    try:
        connector = ETLConnector()
        # Make a simple API request
        data = connector.make_api_request({'pageSize': 1})  # Request just 1 article for testing
        
        if data and data.get('status') == 'ok':
            articles = data.get('articles', [])
            print(f"✓ NewsAPI connection successful - Retrieved {len(articles)} article(s)")
            if articles:
                print(f"  Sample article: {articles[0].get('title', 'No title')[:50]}...")
            return True
        else:
            error_msg = data.get('message', 'Unknown error') if data else 'No response'
            print(f"✗ NewsAPI connection failed: {error_msg}")
            return False
    except Exception as e:
        print(f"✗ NewsAPI connection error: {e}")
        return False


def test_data_extraction():
    """Test data extraction"""
    print("Testing data extraction...")
    try:
        connector = ETLConnector()
        raw_data = connector.extract_data()
        
        if raw_data:
            print(f"✓ Data extraction successful - {len(raw_data)} articles extracted")
            if raw_data:
                sample_article = raw_data[0]
                print(f"  Sample article title: {sample_article.get('title', 'No title')[:50]}...")
                print(f"  Sample article source: {sample_article.get('source', {}).get('name', 'Unknown')}")
            return True
        else:
            print("✗ Data extraction failed - No data returned")
            return False
    except Exception as e:
        print(f"✗ Data extraction error: {e}")
        return False


def test_data_transformation():
    """Test data transformation"""
    print("Testing data transformation...")
    try:
        connector = ETLConnector()
        # Get some sample data
        raw_data = connector.extract_data()
        
        if not raw_data:
            print("✗ Cannot test transformation - No raw data available")
            return False
        
        # Transform the data
        transformed_data = connector.transform_data(raw_data[:5])  # Transform first 5 articles
        
        if transformed_data:
            print(f"✓ Data transformation successful - {len(transformed_data)} articles transformed")
            if transformed_data:
                sample = transformed_data[0]
                print(f"  Sample transformed article:")
                print(f"    Title: {sample.get('title', 'No title')[:50]}...")
                print(f"    Source: {sample.get('source', {}).get('name', 'Unknown')}")
                print(f"    Data quality score: {len([k for k, v in sample.get('data_quality', {}).items() if v])}/6")
            return True
        else:
            print("✗ Data transformation failed - No transformed data")
            return False
    except Exception as e:
        print(f"✗ Data transformation error: {e}")
        return False


def test_full_pipeline():
    """Test the complete ETL pipeline"""
    print("Testing complete ETL pipeline...")
    try:
        connector = ETLConnector()
        success = connector.run_etl_pipeline()
        
        if success:
            print("✓ Complete ETL pipeline test successful")
            return True
        else:
            print("✗ Complete ETL pipeline test failed")
            return False
    except Exception as e:
        print(f"✗ Complete ETL pipeline error: {e}")
        return False


def test_data_validation():
    """Test data validation"""
    print("Testing data validation...")
    try:
        connector = ETLConnector()
        validation_results = connector.validate_data()
        
        if validation_results:
            print("✓ Data validation successful")
            print(f"  Total records: {validation_results.get('total_records', 0)}")
            print(f"  Today's records: {validation_results.get('today_records', 0)}")
            print(f"  Unique sources: {validation_results.get('unique_sources', 0)}")
            return True
        else:
            print("✗ Data validation failed")
            return False
    except Exception as e:
        print(f"✗ Data validation error: {e}")
        return False


def main():
    """Run all tests"""
    print("=" * 60)
    print("NewsAPI ETL Connector - Test Suite")
    print("=" * 60)
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    tests = [
        ("MongoDB Connection", test_mongodb_connection),
        ("NewsAPI Connection", test_api_connection),
        ("Data Extraction", test_data_extraction),
        ("Data Transformation", test_data_transformation),
        ("Full ETL Pipeline", test_full_pipeline),
        ("Data Validation", test_data_validation)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n{'-' * 40}")
        print(f"Running: {test_name}")
        print(f"{'-' * 40}")
        
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
        
        print()
    
    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASSED" if result else "✗ FAILED"
        print(f"{test_name:<25} {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return True
    else:
        print(f"⚠️  {total - passed} test(s) failed")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)