# NewsAPI ETL Data Connector

A robust, production-ready ETL (Extract, Transform, Load) pipeline for ingesting news data from NewsAPI into MongoDB. This connector automatically extracts top headlines, transforms the data for analysis, and loads it into a MongoDB collection with comprehensive data quality tracking.

## 🚀 Features

- Automated News Extraction: Fetches top headlines from NewsAPI
- Data Quality Monitoring: Tracks missing fields, content lengths, and data completeness
- Robust Error Handling: Retry logic, rate limiting, and graceful failure handling
- Duplicate Management: Smart deduplication based on article URLs
- Rich Metadata: Adds ingestion timestamps, data quality scores, and ETL tracking
- Comprehensive Logging: Detailed logs for monitoring and debugging
- Test Suite: Complete test coverage for all pipeline components

## 📋 Prerequisites

- Python 3.7+
- MongoDB instance (local or remote)
- NewsAPI account and API key
- Required Python packages (see requirements.txt)

## 🛠️ Installation

1. Clone the repository
   ```bash
   git clone <repository-url>
   cd newsapi-etl-connector
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables
   Create a `.env` file in the project root:
   ```env
   # NewsAPI Configuration
   API_BASE_URL=https://newsapi.org/v2/top-headlines
   API_KEY=your_newsapi_key_here
   
   # MongoDB Configuration
   MONGO_URI=mongodb://localhost:27017/
   MONGO_DB=etl_database
   COLLECTION_NAME=newsapi
   
   # ETL Configuration
   RATE_LIMIT_DELAY=1.0
   MAX_RETRIES=3
   ```

## 📦 Dependencies

Create a `requirements.txt` file with:
```
requests>=2.25.1
python-dotenv>=0.19.0
pymongo>=4.0.0
urllib3>=1.26.0
```

## 🏃‍♂️ Quick Start

### Basic Usage
```python
from etl_connector import ETLConnector

# Initialize the connector
connector = ETLConnector()

# Run the complete ETL pipeline
success = connector.run_etl_pipeline()

if success:
    print("✅ ETL pipeline completed successfully!")
    
    # Validate the loaded data
    validation_results = connector.validate_data()
    print(f"Total articles loaded: {validation_results.get('total_records', 0)}")
else:
    print("❌ ETL pipeline failed. Check logs for details.")
```

### Running from Command Line
```bash
python etl_connector.py
```

## 🧪 Testing

Run the comprehensive test suite:
```bash
python test_connector.py
```

The test suite includes:
- ✅ MongoDB connection testing
- ✅ NewsAPI connection testing  
- ✅ Data extraction verification
- ✅ Data transformation validation
- ✅ Complete pipeline testing
- ✅ Data quality validation

## 📊 Data Schema

### Raw NewsAPI Response
```json
{
  "status": "ok",
  "totalResults": 34,
  "articles": [
    {
      "title": "Breaking News Title",
      "description": "Article description...",
      "url": "https://example.com/article",
      "urlToImage": "https://example.com/image.jpg",
      "publishedAt": "2025-08-10T12:30:00Z",
      "author": "John Doe",
      "source": {
        "id": "news-source",
        "name": "News Source Name"
      }
    }
  ]
}
```

### Transformed MongoDB Document
```json
{
  "_id": "news_12345_1723291234",
  "title": "Breaking News Title",
  "description": "Article description...",
  "content": "Full article content...",
  "url": "https://example.com/article",
  "url_to_image": "https://example.com/image.jpg",
  "published_at": "2025-08-10T12:30:00Z",
  "published_date": "2025-08-10",
  "published_timestamp": "2025-08-10T12:30:00+00:00",
  "author": "John Doe",
  "source": {
    "id": "news-source",
    "name": "News Source Name"
  },
  "data_quality": {
    "title_length": 19,
    "description_length": 25,
    "content_length": 150,
    "has_image": true,
    "has_author": true,
    "has_description": true
  },
  "etl_metadata": {
    "ingestion_timestamp": "2025-08-10T12:45:40.123456",
    "ingestion_date": "2025-08-10",
    "source": "newsapi",
    "connector_version": "1.0",
    "data_type": "news_article",
    "data_quality_issues": ["missing_content"]
  }
}
```

## ⚙️ Configuration Options

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `API_BASE_URL` | NewsAPI endpoint URL | `https://newsapi.org/v2/top-headlines` | No |
| `API_KEY` | Your NewsAPI key | - | Yes |
| `MONGO_URI` | MongoDB connection string | `mongodb://localhost:27017/` | No |
| `MONGO_DB` | Database name | `etl_database` | No |
| `COLLECTION_NAME` | Collection name | `newsapi` | No |
| `RATE_LIMIT_DELAY` | Delay between API calls (seconds) | `1.0` | No |
| `MAX_RETRIES` | Maximum retry attempts | `3` | No |

### NewsAPI Parameters

The connector supports various NewsAPI parameters:
- `country`: Country code (default: 'us')
- `pageSize`: Articles per request (default: 100, max: 100)
- `category`: News category (business, sports, technology, etc.)
- `sources`: Specific news sources

Modify the `make_api_request` method to customize parameters:
```python
default_params = {
    'country': 'us',
    'category': 'technology',  # Add category
    'pageSize': 50             # Reduce page size
}
```

## 📈 Monitoring & Analytics

### Data Quality Metrics
- Title/Description/Content lengths: Track content richness
- Missing field detection: Monitor data completeness  
- Image availability: Track multimedia content
- Author attribution: Monitor byline availability

### ETL Metrics
- Processing success rate: Track transformation success
- Ingestion volume: Monitor daily/hourly ingestion rates
- Source diversity: Track unique news sources
- Error patterns: Monitor common failure points

### MongoDB Queries for Analytics

Daily ingestion volume:
```javascript
db.newsapi.countDocuments({
  "etl_metadata.ingestion_date": "2025-08-10"
})
```

Top news sources:
```javascript
db.newsapi.aggregate([
  { $group: { _id: "$source.name", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 10 }
])
```

Data quality overview:
```javascript
db.newsapi.aggregate([
  {
    $group: {
      _id: null,
      avgTitleLength: { $avg: "$data_quality.title_length" },
      articlesWithImages: { $sum: { $cond: ["$data_quality.has_image", 1, 0] } },
      totalArticles: { $sum: 1 }
    }
  }
])
```

## 🔧 Troubleshooting

### Common Issues

1. API Key Authentication Error (401)
```
ERROR - Invalid API key
```
- Verify your API key in the `.env` file
- Check NewsAPI dashboard for key status
- Ensure key doesn't have quotes around it

2. Rate Limiting (429)
```
WARNING - Rate limited. Waiting 60 seconds...
```
- Increase `RATE_LIMIT_DELAY` in `.env`
- Consider upgrading your NewsAPI plan
- Monitor your API usage

3. MongoDB Connection Failed
```
ERROR - MongoDB connection failed: ServerSelectionTimeoutError
```
- Verify MongoDB is running
- Check connection string format
- Ensure network connectivity

4. No Data Extracted
```
ERROR - No data extracted
```
- Check NewsAPI status
- Verify country/category parameters
- Check API usage limits

5. Transform Errors
```
ERROR - Error transforming article: 'NoneType' object has no attribute 'strip'
```
- This is handled in the current version
- Check for API response format changes

### Debug Mode

Enable verbose logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## 📋 Production Deployment

### Recommended Production Setup

1. Environment Separation
   ```env
   # Production .env
   MONGO_URI=mongodb://prod-server:27017/
   MONGO_DB=news_production
   RATE_LIMIT_DELAY=2.0
   MAX_RETRIES=5
   ```

2. Scheduled Execution
   ```bash
   # Crontab for hourly execution
   0 * * * * /path/to/python /path/to/etl_connector.py >> /var/log/newsapi-etl.log 2>&1
   ```

3. Monitoring Setup
   - Set up log rotation
   - Monitor disk space for MongoDB
   - Set up alerts for ETL failures
   - Track API usage quotas

### Performance Optimization

- Indexing: The connector creates optimal indexes automatically
- Batch Processing: Processes articles efficiently with bulk operations
- Memory Management: Streams data to handle large datasets
- Connection Pooling: Reuses database connections

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Setup
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
python -m pytest test_connector.py -v

# Run linting
flake8 etl_connector.py
```

## 🙏 Acknowledgments

- [NewsAPI](https://newsapi.org/) for providing comprehensive news data
- [MongoDB](https://www.mongodb.com/) for the robust document database
- [Python](https://www.python.org/) community for excellent libraries

## 📞 Support

For questions, issues, or contributions:
- Author: Shankari S R - 3122225001125


## 📊 Project Stats

- Language: Python 3.7+
- Database: MongoDB
- API: NewsAPI v2
- Test Coverage: 90%+
- Production Ready: ✅

---

