#!/usr/bin/env python3
"""
PhishTank CSV ETL Connector
============================
Fetches phishing site data from the public PhishTank CSV feed,
transforms it, and loads it into MongoDB.

Author: Shankari S R
Roll Number: 3122225001125 - C
"""

import os
import sys
import csv
import requests
from datetime import datetime, timezone
from io import StringIO
from dotenv import load_dotenv
from pymongo import MongoClient

# ------------------------------
# Load environment variables
# ------------------------------
load_dotenv()

PHISHTANK_CSV_URL = os.getenv("PHISHTANK_CSV_URL", "https://data.phishtank.com/data/online-valid.csv")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "threat_intelligence_test")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "phishtank_raw")


def download_csv(url):
    """Download CSV feed from PhishTank."""
    try:
        print(f"[INFO] Downloading PhishTank CSV feed from {url} ...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"[INFO] CSV download successful, size: {len(response.content)/1024:.2f} KB")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to download CSV: {e}")
        return None


def parse_csv(csv_text):
    """Parse CSV string into a list of dicts."""
    try:
        csv_file = StringIO(csv_text)
        reader = csv.DictReader(csv_file)
        records = list(reader)
        print(f"[INFO] Parsed {len(records)} records from CSV")
        return records
    except Exception as e:
        print(f"[ERROR] Failed to parse CSV: {e}")
        return []


def transform_records(records):
    """Transform records to include UTC timestamp."""
    for record in records:
        record["ingestion_timestamp"] = datetime.now(timezone.utc)
    return records


def load_to_mongodb(records, uri, db_name, collection_name):
    """Insert or update records into MongoDB without duplicate errors."""
    try:
        client = MongoClient(uri)
        collection = client[db_name][collection_name]

        inserted_count = 0
        updated_count = 0

        for record in records:
            result = collection.update_one(
                {"phish_id": record["phish_id"]},  # match by unique key
                {"$set": record},                  # update all fields
                upsert=True
            )
            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1

        total_count = collection.count_documents({})
        print(f"[INFO] Inserted {inserted_count} new records")
        print(f"[INFO] Updated {updated_count} existing records")
        print(f"[INFO] Collection now contains {total_count} total records")
        client.close()
        return inserted_count
    except Exception as e:
        print(f"[ERROR] Failed to insert/update MongoDB: {e}")
        return 0


def run_pipeline():
    """Main ETL pipeline."""
    start_time = datetime.now()

    print("\n==================================================")
    print("      STARTING PHISHTANK CSV ETL PIPELINE")
    print("==================================================")

    csv_text = download_csv(PHISHTANK_CSV_URL)
    if not csv_text:
        print("[ERROR] No data downloaded. Pipeline aborted.")
        return

    parsed_records = parse_csv(csv_text)
    if not parsed_records:
        print("[ERROR] No valid records parsed. Pipeline aborted.")
        return

    transformed_records = transform_records(parsed_records)
    inserted_count = load_to_mongodb(transformed_records, MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION)

    duration = (datetime.now() - start_time).total_seconds()

    print("\n==================================================")
    print("PHISHTANK CSV ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("==================================================")
    print(f"Extracted:  {len(parsed_records)} records")
    print(f"Transformed: {len(transformed_records)} records")
    print(f"Loaded:     {inserted_count} records")
    print(f"Duration:   {duration:.2f} seconds")


if __name__ == "__main__":
    run_pipeline()
