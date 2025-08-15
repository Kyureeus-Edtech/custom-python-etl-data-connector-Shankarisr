# PhishTank CSV ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline for processing PhishTank CSV data and storing it in MongoDB for cybersecurity threat intelligence analysis.

**Author:** Shankari S R  
**Roll Number:** 3122225001125  - CSE C
**Course:** Software Architecture - SSN CSE

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Data Schema](#data-schema)
- [Testing](#testing)
- [Monitoring & Logs](#monitoring--logs)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## 🎯 Overview

This ETL pipeline extracts phishing URL data from PhishTank's public CSV feed, transforms it into a structured format with metadata, and efficiently loads it into MongoDB for cybersecurity analysis. PhishTank is a collaborative clearinghouse for phishing data and research maintained by security professionals worldwide.

### Pipeline Architecture

```mermaid
graph LR
    A[PhishTank CSV Feed] --> B[Download & Extract]
    B --> C[Parse CSV Data]
    C --> D[Transform & Enrich]
    D --> E[MongoDB Storage]
    E --> F[Analytics & Reporting]
```

### Key Components
- **Extractor**: Downloads PhishTank CSV feed with retry logic
- **Parser**: Converts CSV data into structured Python dictionaries
- **Transformer**: Adds timestamps and enriches data for analysis
- **Loader**: Performs upsert operations to MongoDB with deduplication
- **Monitor**: Provides comprehensive logging and error handling

## ✨ Features

- 🔄 **Real-time Data Processing**: Fetches live phishing data from PhishTank
- 🛡️ **Duplicate Prevention**: Uses phish_id for intelligent upserts
- 🚀 **High Performance**: Batch processing with optimized MongoDB operations
- 📊 **Data Enrichment**: Adds ingestion timestamps and metadata
- 🔒 **Production Ready**: Comprehensive error handling and logging
- 📈 **Scalable Architecture**: Designed for high-volume threat intelligence workflows
- 🔧 **Easy Configuration**: Environment-based configuration management
- 📝 **Comprehensive Testing**: Full test suite with validation checks

## 📁 Project Structure

```
phishtank-etl/
├── etl_connector.py          # Main ETL pipeline implementation
├── setup.py                  # Automated setup and deployment script
├── test_connector.py         # Comprehensive test suite
├── requirements.txt          # Python dependencies
├── .env                      # Environment configuration
├── .gitignore               # Git ignore patterns
├── README.md                # This documentation
├── logs/                    # Log files directory
│   └── phishtank_etl.log    # Application logs
└── deployment_report.json   # Setup validation report
```

## 🔧 Prerequisites

### System Requirements
- **Python**: 3.8 or higher
- **MongoDB**: 4.4 or higher (local or remote)
- **Network**: Internet access to fetch PhishTank data
- **Storage**: 1GB+ free space (recommended)
- **Memory**: 2GB+ RAM (recommended)

### Python Dependencies
All dependencies are listed in `requirements.txt`:

```txt
pymongo>=4.0.0
requests>=2.28.0
python-dotenv>=0.19.0
pandas>=1.5.0
validators>=0.20.0
```

## 🚀 Installation

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd phishtank-etl-pipeline
```

### 2. Create Virtual Environment
```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

### 3. Run Automated Setup
```bash
python setup.py
```

The setup script will:
- ✅ Check Python version compatibility
- ✅ Install all required dependencies
- ✅ Create environment configuration
- ✅ Test PhishTank feed connectivity
- ✅ Validate MongoDB connection
- ✅ Run sample ETL pipeline
- ✅ Generate deployment report

### 4. Manual Installation (Alternative)
```bash
# Install dependencies
pip install -r requirements.txt

# Create environment file
cp .env.example .env
# Edit .env with your settings
```

## ⚙️ Configuration

### Environment Variables (.env)

```bash
# PhishTank Data Source Configuration
PHISHTANK_CSV_URL=https://data.phishtank.com/data/online-valid.csv
PHISHTANK_UPDATE_INTERVAL_HOURS=24

# MongoDB Database Configuration
MONGODB_URI=mongodb://localhost:27017/
MONGODB_DATABASE=threat_intelligence_test
MONGODB_COLLECTION=phishtank_raw

# Operational Flags
TESTING_MODE=false
USE_SAMPLE_DATA=false
LOG_LEVEL=INFO
```

### Configuration Options

| Variable | Description | Default Value |
|----------|-------------|---------------|
| `PHISHTANK_CSV_URL` | PhishTank CSV feed endpoint | `https://data.phishtank.com/data/online-valid.csv` |
| `MONGODB_URI` | MongoDB connection string | `mongodb://localhost:27017/` |
| `MONGODB_DATABASE` | Target database name | `threat_intelligence_test` |
| `MONGODB_COLLECTION` | Target collection name | `phishtank_raw` |
| `PHISHTANK_UPDATE_INTERVAL_HOURS` | Data refresh interval | `24` |
| `TESTING_MODE` | Enable test mode | `false` |
| `USE_SAMPLE_DATA` | Use sample data instead of live feed | `false` |

### Alternative Data Sources

```bash
# All verified phishing URLs (default)
PHISHTANK_CSV_URL=https://data.phishtank.com/data/online-valid.csv

# Only verified phishing URLs
PHISHTANK_CSV_URL=https://data.phishtank.com/data/verified_online.csv

# Local CSV file for testing
PHISHTANK_CSV_URL=file:///path/to/local/phishtank.csv
```

## 🎮 Usage

### Basic Usage

```bash
# Run the complete ETL pipeline
python etl_connector.py

# The pipeline will:
# 1. Download latest PhishTank CSV data
# 2. Parse CSV into structured records
# 3. Transform data with timestamps
# 4. Load into MongoDB with upsert logic
```

### Expected Output

```
==================================================
      STARTING PHISHTANK CSV ETL PIPELINE
==================================================
[INFO] Downloading PhishTank CSV feed from https://data.phishtank.com/data/online-valid.csv ...
[INFO] CSV download successful, size: 2847.32 KB
[INFO] Parsed 15423 records from CSV
[INFO] Inserted 15423 new records
[INFO] Updated 0 existing records
[INFO] Collection now contains 15423 total records

==================================================
PHISHTANK CSV ETL PIPELINE COMPLETED SUCCESSFULLY
==================================================
Extracted:  15423 records
Transformed: 15423 records
Loaded:     15423 records
Duration:   12.45 seconds
```

### Programmatic Usage

```python
from etl_connector import download_csv, parse_csv, transform_records, load_to_mongodb
from dotenv import load_dotenv
import os

# Load configuration
load_dotenv()

# Run individual components
csv_data = download_csv(os.getenv("PHISHTANK_CSV_URL"))
records = parse_csv(csv_data)
transformed = transform_records(records)
loaded = load_to_mongodb(
    transformed,
    os.getenv("MONGODB_URI"),
    os.getenv("MONGODB_DATABASE"), 
    os.getenv("MONGODB_COLLECTION")
)

print(f"Successfully processed {loaded} records")
```

## 📊 Data Schema

### Input: PhishTank CSV Format

The PhishTank CSV feed contains the following columns:

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `phish_id` | Integer | Unique PhishTank identifier | `8234567` |
| `url` | String | Malicious URL | `http://suspicious-site.com/login` |
| `phish_detail_url` | String | PhishTank detail page | `http://www.phishtank.com/phish_detail.php?phish_id=8234567` |
| `submission_time` | String | ISO datetime of submission | `2024-08-15T10:30:45+00:00` |
| `verified` | String | Verification status | `yes` / `no` |
| `verification_time` | String | ISO datetime of verification | `2024-08-15T11:15:22+00:00` |
| `online` | String | Current status | `yes` / `no` |
| `target` | String | Target organization | `PayPal` / `Microsoft` |

### Output: MongoDB Document Schema

Each document stored in MongoDB has the following structure:

```javascript
{
  "_id": ObjectId("..."),
  "phish_id": "8234567",
  "url": "http://suspicious-site.com/login",
  "phish_detail_url": "http://www.phishtank.com/phish_detail.php?phish_id=8234567",
  "submission_time": "2024-08-15T10:30:45+00:00",
  "verified": "yes",
  "verification_time": "2024-08-15T11:15:22+00:00", 
  "online": "yes",
  "target": "PayPal",
  "ingestion_timestamp": ISODate("2024-08-15T12:00:00.000Z")
}
```

### Key Schema Features

- **Unique Indexing**: Uses `phish_id` as unique identifier for deduplication
- **Timestamp Tracking**: `ingestion_timestamp` tracks when data was processed
- **Upsert Logic**: Updates existing records or inserts new ones based on `phish_id`
- **Data Preservation**: All original PhishTank fields are preserved

## 🧪 Testing

### Automated Testing Suite

Run the comprehensive test suite:

```bash
python test_connector.py
```

### Test Coverage

The test suite validates:

1. **MongoDB Connection Test**
   - Verifies database connectivity
   - Tests authentication and permissions
   
2. **CSV Download Test**
   - Validates PhishTank feed accessibility
   - Tests network connectivity and timeouts
   
3. **CSV Parse Test**
   - Verifies CSV parsing logic
   - Validates data structure integrity
   
4. **Data Transformation Test**
   - Tests timestamp addition
   - Validates data enrichment logic
   
5. **MongoDB Load Test**
   - Tests upsert functionality
   - Validates duplicate handling

### Sample Test Output

```
============================================================
PhishTank CSV ETL Connector - Test Suite
============================================================
Test started at: 2024-08-15 15:30:45

----------------------------------------
Running: MongoDB Connection
----------------------------------------
✓ MongoDB connection successful

---------------------------------------- 
Running: CSV Download
----------------------------------------
✓ CSV download successful - Size: 2847321 bytes

----------------------------------------
Running: CSV Parse
----------------------------------------
✓ CSV parse successful - Parsed 15423 records
  Sample phish_id: 8234567

----------------------------------------
Running: Data Transformation
----------------------------------------
✓ Transformation successful - 5 records
  Sample ingestion_timestamp: 2024-08-15 15:30:46.123456+00:00

----------------------------------------
Running: MongoDB Load
----------------------------------------
✓ MongoDB load executed - Inserted 1 new records

============================================================
TEST SUMMARY
============================================================
MongoDB Connection       ✓ PASSED
CSV Download            ✓ PASSED
CSV Parse               ✓ PASSED
Data Transformation     ✓ PASSED
MongoDB Load            ✓ PASSED

Total: 5/5 tests passed
🎉 All tests passed!
```

## 📝 Monitoring & Logs

### Logging Configuration

The pipeline provides comprehensive logging:

```python
# Logs are written to both console and file
# Log levels: INFO, WARNING, ERROR
# Log format: timestamp - level - message
```

### Log Files

- **Console Output**: Real-time pipeline status
- **File Logging**: Persistent logs in `logs/` directory (if configured)

### Key Metrics Tracked

- **Extraction**: Number of records downloaded
- **Transformation**: Number of records processed
- **Loading**: Number of records inserted/updated
- **Performance**: Pipeline execution duration
- **Errors**: Failed operations and error details

### MongoDB Collection Statistics

Query collection statistics:

```javascript
// Total documents
db.phishtank_raw.countDocuments()

// Recent ingestions
db.phishtank_raw.find().sort({ingestion_timestamp: -1}).limit(10)

// Verification status breakdown
db.phishtank_raw.aggregate([
  {$group: {_id: "$verified", count: {$sum: 1}}}
])

// Target organization analysis
db.phishtank_raw.aggregate([
  {$group: {_id: "$target", count: {$sum: 1}}},
  {$sort: {count: -1}},
  {$limit: 10}
])
```

## 🔍 Troubleshooting

### Common Issues and Solutions

#### 1. MongoDB Connection Issues

**Problem**: `MongoDB connection failed: [Errno 111] Connection refused`

**Solutions**:
```bash
# Check if MongoDB is running
sudo systemctl status mongod

# Start MongoDB service
sudo systemctl start mongod

# Check connection string in .env
MONGODB_URI=mongodb://localhost:27017/
```

#### 2. PhishTank Feed Download Issues

**Problem**: `Failed to download CSV: HTTPError`

**Solutions**:
- Check internet connectivity
- Verify PhishTank feed URL is accessible
- Check for rate limiting (wait and retry)
- Validate DNS resolution

#### 3. CSV Parsing Errors

**Problem**: `Failed to parse CSV: UnicodeDecodeError`

**Solutions**:
```python
# The pipeline handles encoding automatically
# If issues persist, check CSV file integrity
# Verify network download completed successfully
```

#### 4. Memory Issues with Large Datasets

**Problem**: `MemoryError` or slow performance

**Solutions**:
- Implement batch processing (modify load_to_mongodb)
- Increase system memory
- Process data in chunks
- Use streaming CSV parser for very large files

#### 5. Duplicate Key Errors

**Problem**: MongoDB duplicate key errors

**Solutions**:
The pipeline uses upsert logic to handle duplicates automatically:
```python
# Upsert operation prevents duplicates
collection.update_one(
    {"phish_id": record["phish_id"]},  # match by unique key
    {"$set": record},                  # update all fields
    upsert=True
)
```

### Debug Mode

Enable detailed logging:
```bash
# Set environment variable for debug mode
export LOG_LEVEL=DEBUG
python etl_connector.py
```

### Performance Optimization

For large datasets:
```python
# Modify batch size in load_to_mongodb
def load_to_mongodb(records, uri, db_name, collection_name, batch_size=1000):
    # Process records in batches
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        # Process batch...
```

## 📈 Analytics and Reporting

### Sample Analytics Queries

```javascript
// PhishTank Data Analytics Examples

// 1. Phishing trends by verification status
db.phishtank_raw.aggregate([
  {
    $group: {
      _id: "$verified",
      count: { $sum: 1 },
      percentage: { $multiply: [{ $divide: [{ $sum: 1 }, { $size: "$$ROOT" }] }, 100] }
    }
  }
])

// 2. Top targeted organizations
db.phishtank_raw.aggregate([
  { $match: { target: { $ne: null, $ne: "" } } },
  { $group: { _id: "$target", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 20 }
])

// 3. Daily phishing submission trends
db.phishtank_raw.aggregate([
  {
    $group: {
      _id: { 
        $dateToString: { 
          format: "%Y-%m-%d", 
          date: { $dateFromString: { dateString: "$submission_time" } } 
        }
      },
      count: { $sum: 1 }
    }
  },
  { $sort: { _id: -1 } },
  { $limit: 30 }
])

// 4. Online vs offline phishing sites
db.phishtank_raw.aggregate([
  { $group: { _id: "$online", count: { $sum: 1 } } }
])
```

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes
4. Run tests: `python test_connector.py`
5. Commit changes: `git commit -am 'Add feature'`
6. Push to branch: `git push origin feature-name`
7. Submit a Pull Request

### Code Style Guidelines

- Follow PEP 8 Python style guide
- Use meaningful variable and function names
- Add docstrings for all functions
- Include error handling for all external operations
- Write tests for new functionality

### Testing New Features

```bash
# Run full test suite
python test_connector.py

# Test specific functionality
python -c "from etl_connector import download_csv; print('OK' if download_csv('https://data.phishtank.com/data/online-valid.csv') else 'FAIL')"
```

---

**Project Information**  
🎓 **Course**: Software Architecture - SSN CSE  
👨‍💻 **Developer**: Shankari S R (3122225001125)  
📅 **Last Updated**: August 2025

---

