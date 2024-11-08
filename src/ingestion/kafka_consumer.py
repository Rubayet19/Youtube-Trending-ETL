# src/ingestion/kafka_consumer.py
import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

def start_consumer():
    load_dotenv()
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
    topic = os.getenv('KAFKA_TOPIC', 'youtube_trending')
    
    # Initialize consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='youtube_trending_group'
    )
    
    print(f"Starting consumer... listening to topic: {topic}")
    try:
        for message in consumer:
            video_data = message.value
            print(f"\nReceived video: {video_data['title']}")
            print(f"Channel: {video_data['channel_title']}")
            print(f"Views: {video_data['view_count']}")
            print("-" * 50)
    except KeyboardInterrupt:
        print("Stopping the consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()