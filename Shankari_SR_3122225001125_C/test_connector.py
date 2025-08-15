#!/usr/bin/env python3
"""
Test script for PhishTank CSV ETL Connector
"""

import sys
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from etl_connector import download_csv, parse_csv, transform_records, load_to_mongodb
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure etl_connector.py is in the same directory as this script")
    sys.exit(1)

# Load environment variables
load_dotenv()

# Get environment configs
PHISHTANK_CSV_URL = os.getenv("PHISHTANK_CSV_URL")
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_mongodb_connection():
    """Test MongoDB connection"""
    from pymongo import MongoClient
    print("Testing MongoDB connection...")
    try:
        if not MONGODB_URI:
            print("✗ MONGODB_URI environment variable not set")
            return False

        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        client.server_info()
        print("✓ MongoDB connection successful")
        client.close()
        return True
    except Exception as e:
        print(f"✗ MongoDB connection error: {e}")
        return False


def test_csv_download():
    """Test CSV download from PhishTank"""
    print("Testing CSV download...")
    try:
        if not PHISHTANK_CSV_URL:
            print("✗ PHISHTANK_CSV_URL environment variable not set")
            return False

        csv_text = download_csv(PHISHTANK_CSV_URL)
        if csv_text:
            print(f"✓ CSV download successful - Size: {len(csv_text)} bytes")
            return True
        else:
            print("✗ CSV download failed")
            return False
    except Exception as e:
        print(f"✗ CSV download error: {e}")
        return False


def test_csv_parse():
    """Test CSV parsing"""
    print("Testing CSV parse...")
    try:
        csv_text = download_csv(PHISHTANK_CSV_URL)
        records = parse_csv(csv_text)
        if records:
            print(f"✓ CSV parse successful - Parsed {len(records)} records")
            print(f"  Sample phish_id: {records[0].get('phish_id')}")
            return True
        else:
            print("✗ CSV parse returned no records")
            return False
    except Exception as e:
        print(f"✗ CSV parse error: {e}")
        return False


def test_transformation():
    """Test transformation logic"""
    print("Testing transformation...")
    try:
        csv_text = download_csv(PHISHTANK_CSV_URL)
        records = parse_csv(csv_text)
        transformed = transform_records(records[:5])
        if transformed and "ingestion_timestamp" in transformed[0]:
            print(f"✓ Transformation successful - {len(transformed)} records")
            print(f"  Sample ingestion_timestamp: {transformed[0]['ingestion_timestamp']}")
            return True
        else:
            print("✗ Transformation failed")
            return False
    except Exception as e:
        print(f"✗ Transformation error: {e}")
        return False


def test_load():
    """Test MongoDB loading"""
    print("Testing MongoDB load...")
    try:
        csv_text = download_csv(PHISHTANK_CSV_URL)
        records = parse_csv(csv_text)
        transformed = transform_records(records[:1])
        inserted_count = load_to_mongodb(transformed, MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION)
        if inserted_count >= 0:
            print(f"✓ MongoDB load executed - Inserted {inserted_count} new records")
            return True
        else:
            print("✗ MongoDB load failed")
            return False
    except Exception as e:
        print(f"✗ MongoDB load error: {e}")
        return False


def main():
    print("=" * 60)
    print("PhishTank CSV ETL Connector - Test Suite")
    print("=" * 60)
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    tests = [
        ("MongoDB Connection", test_mongodb_connection),
        ("CSV Download", test_csv_download),
        ("CSV Parse", test_csv_parse),
        ("Data Transformation", test_transformation),
        ("MongoDB Load", test_load)
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
    else:
        print(f"⚠️  {total - passed} test(s) failed")


if __name__ == "__main__":
    main()
