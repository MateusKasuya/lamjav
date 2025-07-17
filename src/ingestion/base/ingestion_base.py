"""
Base classes for data ingestion jobs.

This module provides reusable base classes that handle common ingestion patterns,
error handling, and GCS operations.
"""

import json
import os
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


class IngestionMode(Enum):
    """Ingestion execution modes."""
    HISTORICAL = "historical"    # One-time historical backfill
    DAILY = "daily"             # Daily batch processing
    INTRADAY = "intraday"       # Multiple times per day
    REALTIME = "realtime"       # Streaming/real-time


@dataclass
class IngestionConfig:
    """Configuration for ingestion jobs."""
    catalog: str                 # nba, odds
    table: str                  # teams, games, etc
    mode: IngestionMode
    bucket_name: str
    api_rate_limit: Optional[float] = None
    retry_attempts: int = 3
    chunk_size: Optional[int] = None


class BaseIngestionJob(ABC):
    """
    Abstract base class for all ingestion jobs.
    
    Provides common functionality for:
    - Configuration management
    - GCS operations
    - Error handling and retries
    - Logging and monitoring
    """
    
    def __init__(self, config: IngestionConfig):
        """Initialize the ingestion job."""
        self.config = config
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(config.bucket_name)
        
        # Metrics
        self.records_processed = 0
        self.files_uploaded = 0
        self.errors = []
        
    @abstractmethod
    def extract_data(self, **kwargs) -> Any:
        """Extract data from source API. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    def transform_data(self, raw_data: Any) -> List[Dict]:
        """Transform raw API data. Must be implemented by subclasses."""
        pass
    
    def generate_gcs_path(self, suffix: str = None) -> str:
        """Generate standardized GCS path for the data."""
        today = date.today().strftime('%Y-%m-%d')
        
        if suffix:
            filename = f"{self.config.table}_{suffix}_{today}.json"
        else:
            filename = f"{self.config.table}_{today}.json"
            
        return f"{self.config.catalog}/landing/{self.config.table}/{filename}"
    
    def upload_to_gcs(self, data: List[Dict], gcs_path: str) -> bool:
        """Upload data to Google Cloud Storage."""
        try:
            print(f"Uploading to GCS: gs://{self.config.bucket_name}/{gcs_path}")
            
            json_data = json.dumps(data, indent=2, ensure_ascii=False)
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(json_data, content_type="application/json")
            
            self.files_uploaded += 1
            print(f"✅ Successfully uploaded {len(data)} records to {gcs_path}")
            return True
            
        except GoogleCloudError as e:
            error_msg = f"GCS upload failed for {gcs_path}: {str(e)}"
            print(f"❌ {error_msg}")
            self.errors.append(error_msg)
            return False
    
    def run_with_retry(self, operation_func, *args, **kwargs):
        """Execute operation with retry logic."""
        for attempt in range(self.config.retry_attempts):
            try:
                return operation_func(*args, **kwargs)
            except Exception as e:
                if attempt == self.config.retry_attempts - 1:
                    raise e
                print(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                
    def log_execution_summary(self):
        """Log summary of execution."""
        print("\n" + "="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        print(f"Catalog: {self.config.catalog}")
        print(f"Table: {self.config.table}")
        print(f"Mode: {self.config.mode.value}")
        print(f"Records processed: {self.records_processed}")
        print(f"Files uploaded: {self.files_uploaded}")
        print(f"Errors: {len(self.errors)}")
        
        if self.errors:
            print("\nErrors encountered:")
            for error in self.errors:
                print(f"  - {error}")
        
        success_rate = (self.records_processed / max(1, self.records_processed + len(self.errors))) * 100
        print(f"Success rate: {success_rate:.1f}%")
        print("="*60)
    
    @abstractmethod
    def execute(self, **kwargs) -> bool:
        """Main execution method. Must be implemented by subclasses."""
        pass


class HistoricalIngestionJob(BaseIngestionJob):
    """Base class for historical data ingestion jobs."""
    
    def __init__(self, config: IngestionConfig):
        super().__init__(config)
        if config.mode != IngestionMode.HISTORICAL:
            raise ValueError("HistoricalIngestionJob requires HISTORICAL mode")


class DailyIngestionJob(BaseIngestionJob):
    """Base class for daily data ingestion jobs."""
    
    def __init__(self, config: IngestionConfig):
        super().__init__(config)
        if config.mode != IngestionMode.DAILY:
            raise ValueError("DailyIngestionJob requires DAILY mode")


class IntradayIngestionJob(BaseIngestionJob):
    """Base class for intraday data ingestion jobs."""
    
    def __init__(self, config: IngestionConfig):
        super().__init__(config)
        if config.mode != IngestionMode.INTRADAY:
            raise ValueError("IntradayIngestionJob requires INTRADAY mode")
    
    def generate_gcs_path(self, suffix: str = None) -> str:
        """Generate GCS path with timestamp for intraday jobs."""
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
        
        if suffix:
            filename = f"{self.config.table}_{suffix}_{timestamp}.json"
        else:
            filename = f"{self.config.table}_{timestamp}.json"
            
        return f"{self.config.catalog}/landing/{self.config.table}/{filename}" 