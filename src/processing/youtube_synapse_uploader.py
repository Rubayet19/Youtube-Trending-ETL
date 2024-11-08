# src/processing/youtube_synapse_uploader.py
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

class YouTubeSynapseUploader:
    def __init__(self):
        load_dotenv()
        self.project_root = Path(__file__).parent.parent.parent
        
        # Azure credentials
        self.account_url = os.getenv('SYNAPSE_STORAGE_ACCOUNT_URL')
        self.container_name = os.getenv('SYNAPSE_CONTAINER_NAME')
        
        if not self.account_url or not self.container_name:
            print("Error: Missing environment variables!")
            print("Please ensure you have set SYNAPSE_STORAGE_ACCOUNT_URL and SYNAPSE_CONTAINER_NAME in your .env file")
            sys.exit(1)
            
        self._setup_azure_client()
    
    def _setup_azure_client(self):
        """Setup Azure client with proper authentication"""
        try:
            self.credential = AzureCliCredential()
            self.credential.get_token("https://storage.azure.com/.default")
            print("✓ Successfully authenticated using Azure CLI")
            
            self.service_client = DataLakeServiceClient(
                account_url=self.account_url, 
                credential=self.credential
            )
            print("✓ Successfully connected to Azure Storage")
            
            self.file_system_client = self.service_client.get_file_system_client(self.container_name)
            next(self.file_system_client.get_paths(max_results=1), None)
            print(f"✓ Successfully accessed container: {self.container_name}")
            
        except Exception as e:
            print(f"Failed to setup Azure client: {str(e)}")
            sys.exit(1)

    def _convert_to_int64(self, value):
        """Safely convert value to int64, returning 0 for None/null values"""
        try:
            if pd.isna(value) or value is None:
                return np.int64(0)
            return np.int64(value)
        except (ValueError, TypeError):
            return np.int64(0)

    def transform_video_data(self, json_data):
        """Transform raw JSON data into structured DataFrame with proper data types"""
        # Create DataFrame
        df = pd.DataFrame(json_data)
        
        # Define data types for numeric columns
        numeric_columns = {
            'view_count': 'Int64',
            'like_count': 'Int64',
            'comment_count': 'Int64'
        }
        
        # Convert numeric columns
        for col in numeric_columns:
            df[col] = df[col].apply(self._convert_to_int64)
        
        # Convert timestamps
        df['publish_time'] = pd.to_datetime(df['publish_time'])
        df['fetch_time'] = pd.to_datetime(df['fetch_time'])
        
        # Convert duration to seconds
        def parse_duration(duration):
            try:
                hours = minutes = seconds = 0
                if not duration or pd.isna(duration):
                    return 0
                    
                duration = str(duration).replace('PT', '')
                
                if 'H' in duration:
                    hours = int(duration.split('H')[0])
                    duration = duration.split('H')[1]
                if 'M' in duration:
                    minutes = int(duration.split('M')[0])
                    duration = duration.split('M')[1]
                if 'S' in duration:
                    seconds = int(duration.split('S')[0])
                    
                return np.int64(hours * 3600 + minutes * 60 + seconds)
            except Exception:
                return np.int64(0)
            
        df['duration_seconds'] = df['duration'].apply(parse_duration)
        
        # Create separate tags dataframe
        tags_df = df[['video_id', 'tags']].explode('tags').dropna()
        tags_df = tags_df.rename(columns={'tags': 'tag'})
        
        # Ensure video_id is string
        tags_df['video_id'] = tags_df['video_id'].astype(str)
        tags_df['tag'] = tags_df['tag'].astype(str)
        
        # Drop unnecessary columns and ensure data types
        df = df.drop(['tags', 'duration'], axis=1)
        df['video_id'] = df['video_id'].astype(str)
        df['title'] = df['title'].astype(str)
        df['channel_title'] = df['channel_title'].astype(str)
        df['category_id'] = df['category_id'].astype(str)
        
        # Verify data types
        print("\nData types for video_metrics:")
        for col, dtype in df.dtypes.items():
            print(f"  - {col}: {dtype}")
            
        print("\nData types for video_tags:")
        for col, dtype in tags_df.dtypes.items():
            print(f"  - {col}: {dtype}")
        
        return df, tags_df
        
    def upload_to_synapse(self, df, table_name):
        """Upload DataFrame to Synapse storage"""
        # Create temporary directory for parquet files
        temp_dir = self.project_root / 'data' / 'temp'
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate parquet filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        parquet_file = temp_dir / f"{table_name}_{timestamp}.parquet"
        
        try:
            # Save to parquet with specific compression
            df.to_parquet(
                parquet_file,
                index=False,
                engine='pyarrow',
                compression='snappy'
            )
            print(f"✓ Created parquet file: {parquet_file.name}")
            
            # Upload to Synapse
            synapse_path = f"raw/{parquet_file.name}"
            file_client = self.file_system_client.create_file(synapse_path)
            
            with open(parquet_file, 'rb') as file:
                file_content = file.read()
                file_client.append_data(file_content, 0)
                file_client.flush_data(len(file_content))
                
            print(f"✓ Uploaded to Synapse: {synapse_path}")
            
        except Exception as e:
            print(f"Error uploading {parquet_file.name}: {str(e)}")
            raise
        finally:
            # Clean up
            if parquet_file.exists():
                parquet_file.unlink()
            try:
                temp_dir.rmdir()
            except:
                pass
        
    def process_and_upload(self, json_file_name):
        """Main process to transform and upload data"""
        json_path = self.project_root / 'data' / 'raw' / json_file_name
        
        if not json_path.exists():
            print(f"❌ Error: Could not find JSON file at: {json_path}")
            sys.exit(1)
        
        print(f"✓ Found JSON file: {json_path}")
        
        try:
            # Read and process data
            with open(json_path, 'r') as file:
                json_data = json.load(file)
                print(f"✓ Loaded JSON data: {len(json_data)} records")
            
            videos_df, tags_df = self.transform_video_data(json_data)
            print(f"✓ Transformed data into {len(videos_df)} videos and {len(tags_df)} tags")
            
            # Upload to Synapse
            self.upload_to_synapse(videos_df, 'video_metrics')
            self.upload_to_synapse(tags_df, 'video_tags')
            
            print("\n✅ Successfully completed all uploads!")
            
        except Exception as e:
            print(f"\n❌ Error processing data: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        uploader = YouTubeSynapseUploader()
        uploader.process_and_upload('trending_videos_20241104_131757.json')
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)