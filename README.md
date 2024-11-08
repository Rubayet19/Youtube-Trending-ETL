# YouTube Trending Videos Analytics Pipeline

A data pipeline that collects YouTube trending videos data, processes it through Kafka and Spark, stores it in Azure Synapse, and enables analysis through Looker Studio.

## Tech Stack
- **Data Collection**: YouTube Data API v3
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark
- **Storage**: PostgreSQL, Azure Synapse
- **Containerization**: Docker, Docker Compose
- **Visualization**: Looker Studio
- **Language**: Python 3.9+

## Project Structure
```
├── data/
│   ├── looker/          # Prepared data for Looker Studio
│   ├── raw/             # Raw JSON data from YouTube API
│   └── temp/            # Temporary files for processing
├── scripts/
│   └── init_database.sh # Database initialization script
├── src/
│   ├── database/
│   │   └── schema.sql   # Database schema definitions
│   ├── ingestion/
│   │   ├── kafka_consumer.py
│   │   ├── kafka_producer.py
│   │   └── youtube_fetcher.py
│   ├── monitoring/
│   │   ├── pipeline_monitor.py
│   │   └── synapse_data_checker.py
│   ├── processing/
│   │   ├── spark_streaming.py
│   │   └── youtube_synapse_uploader.py
│   └── visualization/
│       └── looker_data.py
├── docker-compose.yml
└── requirements.txt
```

## Setup
1. Clone the repository and install dependencies:
```bash
pip install -r requirements.txt
```

2. Create `.env` file with required credentials:
```env
YOUTUBE_API_KEY=your_youtube_api_key
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
KAFKA_TOPIC=youtube_trending
SYNAPSE_STORAGE_ACCOUNT_URL=your_synapse_url
SYNAPSE_CONTAINER_NAME=your_container_name
```

3. Initialize the database and Kafka:
```bash
./scripts/init_database.sh
```

## Running the Pipeline
1. Start the Kafka producer to fetch YouTube data:
```bash
python src/ingestion/kafka_producer.py
```

2. Start the Spark streaming processor:
```bash
python src/processing/spark_streaming.py
```

3. Monitor the pipeline:
```bash
python src/monitoring/pipeline_monitor.py
```

4. Check data in Synapse:
```bash
python src/monitoring/synapse_data_checker.py
```

5. Prepare data for Looker:
```bash
python src/visualization/looker_data.py
```

## Database Schema
### Video Metrics Table
```sql
CREATE TABLE video_metrics (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20) NOT NULL,
    title TEXT NOT NULL,
    channel_title VARCHAR(255) NOT NULL,
    publish_time TIMESTAMP NOT NULL,
    fetch_time TIMESTAMP NOT NULL,
    processing_time TIMESTAMP NOT NULL,
    view_count BIGINT NOT NULL,
    like_count BIGINT NOT NULL,
    comment_count BIGINT NOT NULL,
    duration_seconds INTEGER NOT NULL,
    category_id VARCHAR(10) NOT NULL
);
```

### Video Tags Table
```sql
CREATE TABLE video_tags (
    id SERIAL PRIMARY KEY,
    video_id VARCHAR(20) NOT NULL,
    tag VARCHAR(255) NOT NULL
);
```

## Key Features
- Real-time trending videos data collection
- Scalable data processing with Kafka and Spark
- Azure Synapse Analytics integration
- Automated monitoring and data quality checks
- Data preparation for Looker Studio visualizations

## Requirements
- Python 3.9+
- Docker and Docker Compose
- YouTube Data API key
- Azure account with Synapse Analytics access
- PostgreSQL 14
