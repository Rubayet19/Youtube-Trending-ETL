from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pandas as pd
import numpy as np
import os
import tempfile
from dotenv import load_dotenv
from pathlib import Path
import json
import csv

class LookerDataPrep:
    def __init__(self):
        load_dotenv()
        self.project_root = Path(__file__).parent.parent.parent
        
        # Azure credentials
        self.account_url = os.getenv('SYNAPSE_STORAGE_ACCOUNT_URL')
        self.container_name = os.getenv('SYNAPSE_CONTAINER_NAME')
        
        # Define expected column orders
        self.expected_columns = [
            'video_id',
            'title',
            'channel_title',
            'publish_time',
            'view_count',
            'like_count',
            'comment_count',
            'category_id',
            'fetch_time',
            'duration_seconds',
            'engagement_rate',
            'publish_date',
            'day_of_week'
        ]
        
        self.expected_tag_columns = [
            'video_id',
            'tag'
        ]
        
        # Initialize Azure client
        self._setup_azure_client()

    def _setup_azure_client(self):
        """Setup Azure client with proper authentication"""
        try:
            self.credential = AzureCliCredential()
            self.service_client = DataLakeServiceClient(
                account_url=self.account_url, 
                credential=self.credential
            )
            self.file_system_client = self.service_client.get_file_system_client(self.container_name)
            print("✓ Successfully connected to Azure Storage")
        except Exception as e:
            print(f"Failed to initialize Azure client: {str(e)}")
            raise

    def _format_dataframe_for_looker(self, df):
        """Format DataFrame to match Looker's expected structure"""
        # Convert data types
        df['video_id'] = df['video_id'].astype(str)
        df['title'] = df['title'].astype(str)
        df['channel_title'] = df['channel_title'].astype(str)
        df['category_id'] = df['category_id'].astype(str)
        df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce').fillna(0).astype(np.int64)
        df['like_count'] = pd.to_numeric(df['like_count'], errors='coerce').fillna(0).astype(np.int64)
        df['comment_count'] = pd.to_numeric(df['comment_count'], errors='coerce').fillna(0).astype(np.int64)
        df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce').fillna(0).astype(np.int64)
        
        # Convert and format timestamps
        df['publish_time'] = pd.to_datetime(df['publish_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['fetch_time'] = pd.to_datetime(df['fetch_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Add calculated columns
        df['engagement_rate'] = ((df['like_count'] + df['comment_count']) / 
                               df['view_count'].replace(0, 1) * 100).round(2)
        df['publish_date'] = pd.to_datetime(df['publish_time']).dt.strftime('%Y-%m-%d')
        df['day_of_week'] = pd.to_datetime(df['publish_time']).dt.day_name()
        
        # Reorder columns to match expected order
        for col in self.expected_columns:
            if col not in df.columns:
                df[col] = ''  # Add missing columns with empty values
        
        return df[self.expected_columns]

    def _format_tags_for_looker(self, df):
        """Format tags DataFrame for Looker"""

        formatted_df = pd.DataFrame()
        
        # Format video_id and tag
        formatted_df['video_id'] = df['video_id'].astype(str).str.strip()
        formatted_df['tag'] = df['tag'].astype(str).str.strip()
        
        # Remove any empty tags
        formatted_df = formatted_df[formatted_df['tag'].str.len() > 0]
        
        # Remove duplicates and reset index
        formatted_df = formatted_df.drop_duplicates().reset_index(drop=True)
        
        return formatted_df[self.expected_tag_columns]

    def process_and_save_data(self):
        """Process and save data in Looker-compatible format"""
        print("\n🔄 Processing data for Looker Studio...")
        
        # Create directory for Looker data
        looker_dir = self.project_root / 'data' / 'looker'
        looker_dir.mkdir(parents=True, exist_ok=True)
        
        # Get latest files from Azure
        paths = list(self.file_system_client.get_paths())
        metrics_files = [p.name for p in paths if "video_metrics" in p.name]
        tags_files = [p.name for p in paths if "video_tags" in p.name]
        
        if not metrics_files or not tags_files:
            raise ValueError("Could not find required files in Azure storage")
            
        latest_metrics = sorted(metrics_files)[-1]
        latest_tags = sorted(tags_files)[-1]
        
        print(f"Processing files:")
        print(f"- Metrics: {latest_metrics}")
        print(f"- Tags: {latest_tags}")
        
        # Download and process data
        metrics_df = self._download_and_read_parquet(latest_metrics)
        tags_df = self._download_and_read_parquet(latest_tags)
        
        # Format data for Looker
        print("\n📊 Formatting data...")
        metrics_df = self._format_dataframe_for_looker(metrics_df)
        tags_df = self._format_tags_for_looker(tags_df)
        
        # Save as CSV with specific formatting
        csv_metrics_path = looker_dir / 'video_metrics.csv'
        csv_tags_path = looker_dir / 'video_tags.csv'
        
        print("\n💾 Saving files...")
        
        # Save metrics
        metrics_df.to_csv(
            csv_metrics_path,
            index=False,
            date_format='%Y-%m-%d %H:%M:%S',
            encoding='utf-8'
        )
        
        # Save tags with specific formatting
        tags_df.to_csv(
            csv_tags_path,
            index=False,
            encoding='utf-8',
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            doublequote=True
        )
        
        print(f"✓ Saved metrics to: {csv_metrics_path}")
        print(f"✓ Saved tags to: {csv_tags_path}")
        
        # Verify the files
        print("\n🔍 Verifying saved files...")
        
        # Verify metrics file
        test_metrics = pd.read_csv(csv_metrics_path)
        print(f"Metrics file columns: {', '.join(test_metrics.columns)}")
        print(f"Metrics rows: {len(test_metrics)}")
        
        # Verify tags file
        test_tags = pd.read_csv(csv_tags_path)
        print(f"Tags file columns: {', '.join(test_tags.columns)}")
        print(f"Tags rows: {len(test_tags)}")
        
        # Print sample of both datasets
        print("\nSample of metrics data:")
        print(test_metrics[['video_id', 'title', 'view_count']].head().to_string())
        
        print("\nSample of tags data:")
        print(test_tags.head().to_string())
        
        return looker_dir

    def _download_and_read_parquet(self, file_name):
        """Download and read a parquet file from Azure"""
        file_client = self.file_system_client.get_file_client(file_name)
        
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            data = file_client.download_file().readall()
            tmp.write(data)
            tmp.flush()
            df = pd.read_parquet(tmp.name)
            os.unlink(tmp.name)
            
        return df

if __name__ == "__main__":
    try:
        print("🚀 Starting Looker data preparation...")
        prep = LookerDataPrep()
        output_dir = prep.process_and_save_data()
        
        print(f"\n✅ Data preparation complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise