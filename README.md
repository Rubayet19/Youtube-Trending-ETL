# YouTube Trending Videos Analytics Pipeline

A data pipeline that collects YouTube trending videos data, processes it through Kafka and Spark, stores it in Azure Synapse, and enables analysis through Looker Studio.

## Architecture
The pipeline follows these steps:
1. Fetches trending videos data from YouTube API 
2. Streams data through Kafka for real-time processing
3. Processes and transforms data using Spark Streaming
4. Stores results in Azure Synapse Analytics
5. Prepares formatted data for Looker Studio visualization

## Tech Stack
- **Data Collection**: YouTube Data API v3
- **Streaming**: Apache Kafka
- **Processing**: Apache Spark
- **Storage**: Azure Synapse Analytics
- **Containerization**: Docker, Docker Compose
- **Visualization**: Looker Studio
- **Language**: Python 3.9+


## Prerequisites

### YouTube API Setup
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable YouTube Data API v3:
   - Go to APIs & Services > Library
   - Search for "YouTube Data API v3"
   - Click Enable
4. Create credentials:
   - Go to APIs & Services > Credentials
   - Click Create Credentials > API Key
   - Copy the API key

### Azure Synapse Setup
1. Create an Azure account if you don't have one
2. Create a Synapse workspace:
   - Go to Azure Portal
   - Search for "Synapse Analytics"
   - Create a new workspace
3. Get storage account details:
   - In your workspace, go to Storage Settings
   - Copy the storage account URL
   - Create a container or use the default one
   - Note down the container name

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

3. Start Kafka:
```bash
docker-compose up -d
```

## Running the Pipeline

1. Start the Kafka producer to fetch YouTube data:
```bash
python src/ingestion/kafka_producer.py
```
This will fetch trending videos every 15 minutes and send them to Kafka.

2. Start the Spark streaming processor:
```bash
python src/processing/spark_streaming.py
```
This processes the data and prepares it for storage.

3. Check data in Synapse:
```bash
python src/monitoring/synapse_data_checker.py
```
Verifies data quality and schema compliance in Azure Synapse.

4. Prepare data for Looker:
```bash
python src/visualization/looker_data.py
```
Formats and prepares data for Looker Studio visualization.



## Data Structure
The pipeline processes and stores two main data types:

### Video Metrics
Data about each trending video:
- video_id: Unique video identifier
- title: Video title
- channel_title: Channel name
- publish_time: When video was published
- fetch_time: When data was collected
- processing_time: When data was processed
- view_count: Total views
- like_count: Total likes
- comment_count: Total comments
- duration_seconds: Video duration
- category_id: YouTube category ID

### Video Tags
Tags associated with each video:
- video_id: Video identifier
- tag: Individual tag text


## Visualization in Looker Studio
After preparing the data using `looker_data.py`, you'll find two CSV files in the `data/looker` directory:
- `video_metrics.csv`: Contains all video metrics
- `video_tags.csv`: Contains video tags data

To create visualizations:

1. Go to [Looker Studio](https://lookerstudio.google.com/)
2. Click "Create" and select "New Report"
3. Click "Create new data source" and select "File Upload"
4. First upload `video_metrics.csv`:
   - Set date fields: publish_time, fetch_time, processing_time
   - Ensure numeric fields (view_count, like_count, etc.) are set as numbers
   - Click "Connect"

5. Add `video_tags.csv` as an additional data source:
   - Click "Add data" in the Resources menu
   - Upload `video_tags.csv`
   - Ensure both video_id and tag are set as text fields
   - Click "Connect"

6. Create a blend:
   - Click "Add a Blend"
   - Select both data sources
   - Join using "video_id" as the join key
   - Click "Save"

7. You can now create various visualizations:
   - Trending videos by view count
   - Tag clouds for popular topics
   - Channel performance metrics
   - Time-based trends
   - Engagement rate analysis
   - Category distribution


It will look something like this:

![Screenshot 2024-11-07 at 8 07 15 PM](https://github.com/user-attachments/assets/7374709a-b690-4382-8e44-a6c8f5eb1413)

It can be viewed here:
https://lookerstudio.google.com/reporting/1a5ffb1c-70f4-4123-8ecd-079740a55d3b/page/YEiME/edit

## Requirements
- Python 3.9+
- Docker and Docker Compose
- YouTube Data API key
- Azure account with Synapse Analytics access
