#!/usr/bin/env python3
"""
Setup and Deployment Script for PhishTank ETL Connector (CSV Mode)
===================================================================
Automated setup script to prepare the environment and validate the setup.

Author: Shankari S R
Roll Number: 3122225001125 - C
Modified for PhishTank CSV feed ingestion
"""

import os
import sys
import subprocess
import json
import requests
import csv
from io import StringIO
from pathlib import Path


def print_header(title):
    """Print formatted header"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60)


def print_step(step, description):
    """Print formatted step"""
    print(f"\n[Step {step}] {description}")
    print("-" * 40)


def check_python_version():
    """Check if Python version is compatible"""
    print_step(1, "Checking Python Version")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required. Current version:", sys.version)
        return False
    print(f"✅ Python version: {sys.version}")
    return True


def install_dependencies():
    """Install required Python packages"""
    print_step(2, "Installing Dependencies")
    try:
        if not Path('requirements.txt').exists():
            print("❌ requirements.txt not found!")
            return False
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✅ All dependencies installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install dependencies: {e}")
        return False


def setup_environment():
    """Setup environment variables for PhishTank CSV feed"""
    print_step(3, "Setting up Environment")
    if Path('.env').exists():
        print("✅ .env file already exists")
        return True

    env_template = """# PhishTank CSV Feed Configuration
PHISHTANK_CSV_URL=https://data.phishtank.com/data/online-valid.csv
PHISHTANK_UPDATE_INTERVAL_HOURS=24

# MongoDB Configuration
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=threat_intelligence_test
MONGODB_COLLECTION=phishtank_raw

# Logging
LOG_LEVEL=INFO
"""
    try:
        with open('.env', 'w') as f:
            f.write(env_template)
        print("✅ Created .env file for PhishTank CSV configuration")
        return True
    except Exception as e:
        print(f"❌ Failed to create .env file: {e}")
        return False


def test_feed_connectivity():
    """Test connectivity to PhishTank CSV feed"""
    print_step(4, "Testing PhishTank Feed Connectivity")
    try:
        from dotenv import load_dotenv
        load_dotenv()
        feed_url = os.getenv("PHISHTANK_CSV_URL")
        if not feed_url:
            print("❌ PHISHTANK_CSV_URL not found in .env")
            return False
        response = requests.get(feed_url, timeout=15)
        if response.status_code == 200:
            print(f"✅ Feed accessible, size: {len(response.content) / 1024:.1f} KB")
            return True
        else:
            print(f"⚠️  Feed returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Feed connectivity test failed: {e}")
        return False


def test_mongodb_connection():
    """Test MongoDB connectivity"""
    print_step(5, "Testing MongoDB Connectivity")
    try:
        from pymongo import MongoClient
        from dotenv import load_dotenv
        load_dotenv()
        mongo_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f"✅ MongoDB connection successful")
        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        return False


def validate_project_structure():
    """Validate project file structure"""
    print_step(6, "Validating Project Structure")
    required_files = [
        'etl_connector.py',
        'requirements.txt',
        '.env',
        'README.md',
        '.gitignore'
    ]
    missing_files = []
    for file in required_files:
        if Path(file).exists():
            print(f"✅ {file}")
        else:
            print(f"❌ {file} - MISSING")
            missing_files.append(file)
    if missing_files:
        print(f"\n⚠️  Missing files: {', '.join(missing_files)}")
        return False
    print("\n✅ All required files present")
    return True


def run_sample_etl():
    """Run a sample PhishTank CSV ETL pipeline"""
    print_step(7, "Running Sample PhishTank ETL Pipeline")
    try:
        from dotenv import load_dotenv
        from pymongo import MongoClient
        load_dotenv()

        # Fetch CSV feed
        feed_url = os.getenv("PHISHTANK_CSV_URL")
        mongo_uri = os.getenv("MONGODB_URI")
        mongo_db = os.getenv("MONGODB_DATABASE")
        mongo_col = os.getenv("MONGODB_COLLECTION")

        print("🚀 Downloading phishing data (CSV)...")
        response = requests.get(feed_url, timeout=15)
        csv_text = response.text

        # Parse CSV into list of dicts
        reader = csv.DictReader(StringIO(csv_text))
        data = list(reader)

        print(f"✅ Downloaded {len(data)} phishing records from CSV")

        # Insert into MongoDB
        client = MongoClient(mongo_uri)
        col = client[mongo_db][mongo_col]
        col.insert_many(data)
        print(f"✅ Inserted {len(data)} records into MongoDB collection '{mongo_col}'")
        return True
    except Exception as e:
        print(f"❌ Sample ETL failed: {e}")
        return False


def generate_deployment_report():
    """Generate deployment report"""
    print_step(8, "Generating Deployment Report")
    report = {
        "python_version": sys.version,
        "project_path": str(Path.cwd()),
        "setup_status": "completed",
        "etl_type": "PhishTank CSV"
    }
    try:
        with open('deployment_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        print("✅ Deployment report saved to deployment_report.json")
        return True
    except Exception as e:
        print(f"❌ Failed to generate report: {e}")
        return False


def main():
    print_header("PhishTank ETL Connector (CSV Mode) - Setup & Validation")
    print("This script will set up and validate your PhishTank CSV ETL connector environment.")

    steps = [
        ("Python Version Check", check_python_version),
        ("Install Dependencies", install_dependencies),
        ("Environment Setup", setup_environment),
        ("Feed Connectivity Test", test_feed_connectivity),
        ("MongoDB Connection Test", test_mongodb_connection),
        ("Project Structure Validation", validate_project_structure),
        ("Sample ETL Execution", run_sample_etl),
        ("Deployment Report", generate_deployment_report)
    ]

    results = []
    for step_name, step_func in steps:
        try:
            result = step_func()
            results.append((step_name, result))
        except KeyboardInterrupt:
            print("\n❌ Setup interrupted by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Unexpected error in {step_name}: {e}")
            results.append((step_name, False))

    print_header("Setup Summary")
    success_count = 0
    for step_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{step_name:<30} {status}")
        if success:
            success_count += 1

    print(f"\nOverall: {success_count}/{len(results)} steps completed successfully")
    if success_count == len(results):
        print("\n🎉 Setup completed successfully!")
        print("You can now run: python etl_connector.py")
    else:
        print(f"\n⚠️  {len(results) - success_count} steps failed.")
        print("Check README.md for troubleshooting.")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
