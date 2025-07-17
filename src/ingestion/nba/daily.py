"""
NBA daily ingestion jobs.

This module contains daily ingestion jobs for NBA data that should be
run once or twice per day to capture updates.
"""

from datetime import date
from typing import Any, Dict, List

from src.ingestion.base.ingestion_base import DailyIngestionJob, IngestionConfig, IngestionMode
from src.ingestion.nba.client import NBAIngestionJob


class NBATeamsDaily(NBAIngestionJob, DailyIngestionJob):
    """Daily ingestion for NBA teams data."""
    
    def extract_data(self, **kwargs) -> List[Dict]:
        """Extract teams data from NBA API."""
        return self.nba_client.get_teams()
    
    def execute(self, **kwargs) -> bool:
        """Execute daily teams ingestion."""
        try:
            print(f"Starting daily NBA teams ingestion...")
            
            # Extract data
            teams_data = self.run_with_retry(self.extract_data)
            
            # Transform data (already in correct format)
            transformed_data = self.transform_data(teams_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS
            gcs_path = self.generate_gcs_path()
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA teams daily ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


class NBAPlayersDaily(NBAIngestionJob, DailyIngestionJob):
    """Daily ingestion for NBA players data."""
    
    def extract_data(self, active_only: bool = False, **kwargs) -> List[Dict]:
        """Extract players data from NBA API."""
        return self.nba_client.get_players(active_only=active_only)
    
    def execute(self, active_only: bool = False, **kwargs) -> bool:
        """Execute daily players ingestion."""
        try:
            print(f"Starting daily NBA players ingestion (active_only={active_only})...")
            
            # Extract data
            players_data = self.run_with_retry(self.extract_data, active_only=active_only)
            
            # Transform data
            transformed_data = self.transform_data(players_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS
            suffix = "active" if active_only else "all"
            gcs_path = self.generate_gcs_path(suffix=suffix)
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA players daily ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


class NBAGamesDaily(NBAIngestionJob, DailyIngestionJob):
    """Daily ingestion for NBA games data."""
    
    def extract_data(self, target_date: date = None, **kwargs) -> List[Dict]:
        """Extract games data for a specific date."""
        if target_date is None:
            target_date = date.today()
        return self.nba_client.get_games_for_date(target_date)
    
    def execute(self, target_date: date = None, **kwargs) -> bool:
        """Execute daily games ingestion."""
        try:
            if target_date is None:
                target_date = date.today()
                
            print(f"Starting daily NBA games ingestion for {target_date}...")
            
            # Extract data
            games_data = self.run_with_retry(self.extract_data, target_date=target_date)
            
            if not games_data:
                print(f"No games found for {target_date}")
                return True
            
            # Transform data
            transformed_data = self.transform_data(games_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS
            gcs_path = self.generate_gcs_path()
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA games daily ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


class NBAPlayerStatsDaily(NBAIngestionJob, DailyIngestionJob):
    """Daily ingestion for NBA player stats data."""
    
    def extract_data(self, target_date: date = None, **kwargs) -> List[Dict]:
        """Extract player stats data for a specific date."""
        if target_date is None:
            target_date = date.today()
        return self.nba_client.get_player_stats_for_date(target_date)
    
    def execute(self, target_date: date = None, **kwargs) -> bool:
        """Execute daily player stats ingestion."""
        try:
            if target_date is None:
                target_date = date.today()
                
            print(f"Starting daily NBA player stats ingestion for {target_date}...")
            
            # Extract data
            stats_data = self.run_with_retry(self.extract_data, target_date=target_date)
            
            if not stats_data:
                print(f"No player stats found for {target_date}")
                return True
            
            # Transform data
            transformed_data = self.transform_data(stats_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS
            gcs_path = self.generate_gcs_path()
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA player stats daily ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


class NBAPlayerInjuriesDaily(NBAIngestionJob, DailyIngestionJob):
    """Daily ingestion for NBA player injuries data."""
    
    def extract_data(self, **kwargs) -> List[Dict]:
        """Extract current player injuries data."""
        return self.nba_client.get_player_injuries()
    
    def execute(self, **kwargs) -> bool:
        """Execute daily player injuries ingestion."""
        try:
            print(f"Starting daily NBA player injuries ingestion...")
            
            # Extract data
            injuries_data = self.run_with_retry(self.extract_data)
            
            # Transform data (even if empty)
            transformed_data = self.transform_data(injuries_data)
            self.records_processed = len(transformed_data)
            
            # Upload to GCS (always upload, even if empty)
            gcs_path = self.generate_gcs_path()
            success = self.upload_to_gcs(transformed_data, gcs_path)
            
            if not transformed_data:
                print("No injuries found - this is normal if no players are injured")
            
            self.log_execution_summary()
            return success
            
        except Exception as e:
            print(f"Error in NBA player injuries daily ingestion: {str(e)}")
            self.errors.append(str(e))
            self.log_execution_summary()
            return False


# Factory function to create daily jobs
def create_daily_job(table: str, bucket_name: str, **kwargs):
    """Factory function to create daily NBA ingestion jobs."""
    
    config = IngestionConfig(
        catalog="nba",
        table=table,
        mode=IngestionMode.DAILY,
        bucket_name=bucket_name,
        api_rate_limit=kwargs.get('api_rate_limit', 1.0),
        retry_attempts=kwargs.get('retry_attempts', 3)
    )
    
    job_classes = {
        "teams": NBATeamsDaily,
        "players": NBAPlayersDaily,
        "games": NBAGamesDaily,
        "game_player_stats": NBAPlayerStatsDaily,
        "player_injuries": NBAPlayerInjuriesDaily,
    }
    
    if table not in job_classes:
        raise ValueError(f"Unknown table: {table}. Available: {list(job_classes.keys())}")
    
    return job_classes[table](config) 