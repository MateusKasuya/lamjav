"""
NBA intraday ingestion jobs.

This module contains intraday ingestion jobs for NBA data that should be
run multiple times per day to capture real-time updates.
"""

from datetime import date
from typing import Any, Dict, List

from src.ingestion.base.ingestion_base import IntradayIngestionJob, IngestionConfig, IngestionMode
from src.ingestion.nba.client import NBAIngestionJob


class NBAGamesIntraday(NBAIngestionJob, IntradayIngestionJob):
    """Intraday ingestion for NBA games (for live score updates)."""
    
    def extract_data(self, target_date: date = None, **kwargs) -> List[Dict]:
        """Extract games data for a specific date."""
        if target_date is None:
            target_date = date.today()
        return self.nba_client.get_games_for_date(target_date)
    
    def execute(self, target_date: date = None, **kwargs) -> bool:
        """Execute intraday games ingestion."""
        try:
            if target_date is None:
                target_date = date.today()
                
            print(f"Starting intraday NBA games ingestion for {target_date}...")
            
            # Extract data
            games_data = self.run_with_retry(self.extract_data, target_date=target_date)
            
            if not games_data:
                print(f"No games found for {target_date}")
                return True
            
            # Transform data
            transformed_data = self.transform_data(games_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS with timestamp
            gcs_path = self.generate_gcs_path()  # This includes timestamp for intraday
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA games intraday ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


class NBAPlayerInjuriesIntraday(NBAIngestionJob, IntradayIngestionJob):
    """Intraday ingestion for NBA player injuries (for injury updates)."""
    
    def extract_data(self, **kwargs) -> List[Dict]:
        """Extract current player injuries data."""
        return self.nba_client.get_player_injuries()
    
    def execute(self, **kwargs) -> bool:
        """Execute intraday player injuries ingestion."""
        try:
            print(f"Starting intraday NBA player injuries ingestion...")
            
            # Extract data
            injuries_data = self.run_with_retry(self.extract_data)
            
            # Transform data (even if empty)
            transformed_data = self.transform_data(injuries_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS with timestamp
            gcs_path = self.generate_gcs_path()
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            if not transformed_data:
                print("No injuries found - this is normal if no players are injured")
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA player injuries intraday ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


# Factory function for intraday jobs
def create_intraday_job(table: str, bucket_name: str, **kwargs):
    """Factory function to create intraday NBA ingestion jobs."""
    
    config = IngestionConfig(
        catalog="nba",
        table=table,
        mode=IngestionMode.INTRADAY,
        bucket_name=bucket_name,
        api_rate_limit=kwargs.get('api_rate_limit', 0.5),  # Faster for intraday
        retry_attempts=kwargs.get('retry_attempts', 2)     # Fewer retries for speed
    )
    
    job_classes = {
        "games": NBAGamesIntraday,
        "player_injuries": NBAPlayerInjuriesIntraday,
    }
    
    if table not in job_classes:
        raise ValueError(f"Unknown intraday table: {table}. Available: {list(job_classes.keys())}")
    
    return job_classes[table](config) 