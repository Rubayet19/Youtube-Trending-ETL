# src/ingestion/youtube_fetcher.py
import os
from datetime import datetime
from typing import Dict, List
import json


from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

class YouTubeTrendingFetcher:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('YOUTUBE_API_KEY')
        self.youtube = build('youtube', 'v3', developerKey=self.api_key)
        
    def fetch_trending_videos(self, region_code: str = 'US', max_results: int = 50) -> List[Dict]:
        """
        Fetch trending videos from YouTube API
        """
        try:
            request = self.youtube.videos().list(
                part='snippet,contentDetails,statistics',
                chart='mostPopular',
                regionCode=region_code,
                maxResults=max_results
            )
            response = request.execute()
            
            # Process and transform the response
            trending_videos = []
            for item in response['items']:
                video_data = {
                    'video_id': item['id'],
                    'title': item['snippet']['title'],
                    'channel_title': item['snippet']['channelTitle'],
                    'publish_time': item['snippet']['publishedAt'],
                    'view_count': item['statistics'].get('viewCount', 0),
                    'like_count': item['statistics'].get('likeCount', 0),
                    'comment_count': item['statistics'].get('commentCount', 0),
                    'duration': item['contentDetails']['duration'],
                    'tags': item['snippet'].get('tags', []),
                    'category_id': item['snippet']['categoryId'],
                    'fetch_time': datetime.utcnow().isoformat()
                }
                trending_videos.append(video_data)
            
            return trending_videos
            
        except HttpError as e:
            print(f'An HTTP error {e.resp.status} occurred: {e.content}')
            return []
        except Exception as e:
            print(f'An error occurred: {str(e)}')
            return []

    def save_to_file(self, videos: List[Dict], filename: str):
        """
        Save fetched videos to a JSON file
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(videos, f, ensure_ascii=False, indent=2)

# Test the fetcher
if __name__ == '__main__':
    fetcher = YouTubeTrendingFetcher()
    videos = fetcher.fetch_trending_videos()
    
    # Save to a timestamped file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'data/raw/trending_videos_{timestamp}.json'
    
    # Create directory if it doesn't exist
    os.makedirs('data/raw', exist_ok=True)
    
    fetcher.save_to_file(videos, filename)
    print(f'Fetched {len(videos)} videos and saved to {filename}')