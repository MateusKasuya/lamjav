"""
Unified NBA API client.

This module provides a single client interface for all NBA data sources,
centralizing API operations and reducing code duplication.
"""

import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from lib_dev.balldontlie import BalldontlieLib
from src.ingestion.base.ingestion_base import BaseIngestionJob, IngestionConfig


class NBAClient:
    """
    Unified client for NBA data operations.
    
    Centralizes all NBA API calls and provides consistent interface
    for different types of data ingestion jobs.
    """
    
    def __init__(self, api_rate_limit: Optional[float] = None):
        """Initialize NBA client."""
        self.api_client = BalldontlieLib()
        self.rate_limit = api_rate_limit or 1.0  # Default 1 second between calls
        self.last_api_call = 0
        
    def _rate_limit_wait(self):
        """Implement rate limiting between API calls."""
        if self.rate_limit:
            elapsed = time.time() - self.last_api_call
            if elapsed < self.rate_limit:
                time.sleep(self.rate_limit - elapsed)
        self.last_api_call = time.time()
    
    def get_teams(self) -> List[Dict]:
        """Get all NBA teams."""
        self._rate_limit_wait()
        
        response = self.api_client.get_teams()
        if response is None:
            raise Exception("Failed to fetch teams data from API")
            
        return [team.model_dump() for team in response]
    
    def get_players(self, active_only: bool = False) -> List[Dict]:
        """Get NBA players."""
        self._rate_limit_wait()
        
        if active_only:
            response = self.api_client.get_active_players()
        else:
            response = self.api_client.get_players()
            
        if response is None or len(response) == 0:
            raise Exception("Failed to fetch players data from API")
            
        return [player.model_dump() for player in response]
    
    def get_games_for_date(self, target_date: date) -> List[Dict]:
        """Get games for a specific date."""
        self._rate_limit_wait()
        
        response = self.api_client.get_games(target_date)
        if response is None:
            return []
            
        return [game.model_dump() for game in response]
    
    def get_games_for_date_range(self, start_date: date, end_date: date) -> Dict[str, List[Dict]]:
        """Get games for a date range. Returns dict with date as key."""
        games_by_date = {}
        current_date = start_date
        
        while current_date <= end_date:
            print(f"Fetching games for {current_date}")
            
            games = self.get_games_for_date(current_date)
            if games:
                games_by_date[current_date.strftime('%Y-%m-%d')] = games
                
            current_date += timedelta(days=1)
            
        return games_by_date
    
    def get_player_stats_for_date(self, target_date: date) -> List[Dict]:
        """Get player stats for a specific date."""
        self._rate_limit_wait()
        
        response = self.api_client.get_stats(target_date)
        if response is None:
            return []
            
        return [stat.model_dump() for stat in response]
    
    def get_player_stats_for_date_range(self, start_date: date, end_date: date) -> Dict[str, List[Dict]]:
        """Get player stats for a date range."""
        stats_by_date = {}
        current_date = start_date
        
        while current_date <= end_date:
            print(f"Fetching player stats for {current_date}")
            
            stats = self.get_player_stats_for_date(current_date)
            if stats:
                stats_by_date[current_date.strftime('%Y-%m-%d')] = stats
                
            current_date += timedelta(days=1)
            
        return stats_by_date
    
    def get_player_injuries(self) -> List[Dict]:
        """Get current player injuries."""
        self._rate_limit_wait()
        
        response = self.api_client.get_injuries()
        if response is None:
            return []
            
        return [injury.model_dump() for injury in response]
    
    def get_season_averages(self, season: int = 2024, category: str = "general", 
                           season_type: str = "regular", type_param: str = "base") -> List[Dict]:
        """Get season averages."""
        self._rate_limit_wait()
        
        response = self.api_client.get_season_averages(category, season_type, type_param, season)
        if response is None or len(response) == 0:
            raise Exception("Failed to fetch season averages data")
            
        # Handle both Pydantic objects and dicts
        if response and hasattr(response[0], "model_dump"):
            return [avg.model_dump() for avg in response]
        else:
            return response
    
    def get_team_standings(self, season: int = 2024) -> List[Dict]:
        """Get team standings for a season."""
        self._rate_limit_wait()
        
        response = self.api_client.get_team_standings(season)
        if response is None or len(response) == 0:
            raise Exception("Failed to fetch team standings data")
            
        return [standing.model_dump() for standing in response]


class NBAIngestionJob(BaseIngestionJob):
    """Base class for NBA-specific ingestion jobs."""
    
    def __init__(self, config: IngestionConfig):
        super().__init__(config)
        self.nba_client = NBAClient(api_rate_limit=config.api_rate_limit)
    
    def transform_data(self, raw_data: Any) -> List[Dict]:
        """Default transformation - data is already in dict format."""
        if isinstance(raw_data, list):
            return raw_data
        elif isinstance(raw_data, dict):
            return [raw_data]
        else:
            return [] 