# src/ingestion/kafka_producer.py
import json
import os
import time
from typing import Dict, List
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
from youtube_fetcher import YouTubeTrendingFetcher

class YouTubeKafkaProducer:
    def __init__(self):
        load_dotenv()
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
        self.topic = os.getenv('KAFKA_TOPIC', 'youtube_trending')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            # Enable retry if Kafka is not immediately available
            retries=5
        )
        
    def send_to_kafka(self, video_data: Dict):
        """Send a single video record to Kafka"""
        try:
            future = self.producer.send(self.topic, value=video_data)
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            print(f"Sent video {video_data['video_id']} to topic {self.topic} "
                  f"partition {record_metadata.partition} offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            print(f"Failed to send video {video_data['video_id']}: {str(e)}")
            return False
            
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()

def main():
    # Initialize fetcher and producer
    fetcher = YouTubeTrendingFetcher()
    producer = YouTubeKafkaProducer()
    
    try:
        while True:
            print("Fetching trending videos...")
            videos = fetcher.fetch_trending_videos()
            
            if videos:
                print(f"Fetched {len(videos)} videos, sending to Kafka...")
                for video in videos:
                    producer.send_to_kafka(video)
                print("Batch complete")
            else:
                print("No videos fetched")
            
            # Wait for 15 minutes before next fetch
            # YouTube trending data doesn't update very frequently
            print("Waiting for 15 minutes before next fetch...")
            time.sleep(900)  # 15 minutes
            
    except KeyboardInterrupt:
        print("Stopping the producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()