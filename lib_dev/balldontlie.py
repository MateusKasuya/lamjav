"""
Balldontlie API client library.

This module provides a wrapper around the Balldontlie API for NBA data retrieval.
"""

from balldontlie import BalldontlieAPI
from balldontlie.exceptions import (
    AuthenticationError,
    RateLimitError,
    ValidationError,
    NotFoundError,
    ServerError,
    BallDontLieException,
)
from dotenv import load_dotenv
import os
from typing import List, Optional, Any, Dict, Callable, TypeVar
import time
from datetime import date
import requests
from pathlib import Path

# Load .env from project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")

T = TypeVar("T")


class BalldontlieLib:
    """
    A wrapper class for the Balldontlie API.

    This class provides methods to interact with the Balldontlie API
    for retrieving NBA data such as teams, players, and games.
    """

    def __init__(self) -> None:
        """
        Initialize the Balldontlie API client.

        Initializes the API client using the BALLDONTLIE_API_KEY
        environment variable.

        Raises:
            ValueError: If the API key is not found in environment variables
        """
        api_key = os.getenv("BALLDONTLIE_API_KEY")
        if not api_key:
            raise ValueError("BALLDONTLIE_API_KEY environment variable is required")

        self.api = BalldontlieAPI(api_key=api_key)

    def _handle_api_exceptions(self, e: Exception, operation: str) -> None:
        """
        Handle API exceptions in a centralized way.

        Args:
            e: The exception that occurred
            operation: Description of the operation that failed
        """
        if isinstance(e, AuthenticationError):
            print(
                f"Invalid API key during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        elif isinstance(e, RateLimitError):
            print(
                f"Rate limit exceeded during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        elif isinstance(e, ValidationError):
            print(
                f"Invalid request parameters during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        elif isinstance(e, NotFoundError):
            print(
                f"Resource not found during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        elif isinstance(e, ServerError):
            print(
                f"API server error during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        elif isinstance(e, BallDontLieException):
            print(
                f"General API error during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        else:
            print(f"Unexpected error during {operation}: {str(e)}")

    def _handle_rate_limit_with_retry(
        self,
        operation: Callable,
        max_retries: int = 5,
        base_delay: int = 2,
        extra_delay: int = 0,
    ) -> Optional[T]:
        """
        Execute an operation with rate limit handling and retry logic.

        Args:
            operation: The operation to execute
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff
            extra_delay: Additional delay to add to backoff

        Returns:
            Result of the operation if successful, None if failed
        """
        retry_count = 0

        while retry_count < max_retries:
            try:
                return operation()
            except RateLimitError:
                retry_count += 1
                delay = base_delay**retry_count + extra_delay
                print(
                    f"Rate limit hit. Retrying in {delay} seconds... (Attempt {retry_count}/{max_retries})"
                )
                time.sleep(delay)

                if retry_count >= max_retries:
                    print("Max retries reached for rate limit. Crashing.")
                    raise RateLimitError(
                        "Rate limit exceeded after maximum retries", 429, {}
                    )
            except Exception as e:
                print(f"Unexpected error: {str(e)}")
                break

        return None

    def _paginate_with_rate_limit(
        self,
        fetch_page: Callable,
        operation_name: str,
        per_page: int = 25,
        max_retries: int = 5,
        base_delay: int = 2,
        extra_delay: int = 0,
        page_delay: int = 3,
    ) -> Optional[List[Any]]:
        """
        Generic pagination method with rate limit handling.

        Args:
            fetch_page: Function that fetches a single page (should return response with data and meta)
            operation_name: Name of the operation for logging
            per_page: Number of items per page
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff
            extra_delay: Additional delay to add to backoff
            page_delay: Delay between successful page fetches

        Returns:
            List of all items if successful, None if an error occurs
        """
        try:
            print(f"Getting {operation_name}...")
            all_items = []
            cursor = None

            while True:
                retry_count = 0
                data = None

                while retry_count < max_retries:
                    try:
                        # Prepare parameters for the API call
                        params = {"per_page": per_page}

                        # Add cursor only if it's not None
                        if cursor is not None:
                            params["cursor"] = cursor

                        response = fetch_page(**params)
                        data = response.data

                        if not data:
                            print(f"No more {operation_name} found")
                            break

                        all_items.extend(data)
                        print(
                            f"Fetched {len(data)} {operation_name}. Total: {len(all_items)}"
                        )

                        # Check if there are more pages
                        if (
                            hasattr(response.meta, "next_cursor")
                            and response.meta.next_cursor
                        ):
                            cursor = response.meta.next_cursor
                        else:
                            print(f"No more pages for {operation_name}")
                            cursor = None
                            break

                        # Add delay to respect API rate limits
                        time.sleep(page_delay)
                        break  # Success, exit retry loop

                    except RateLimitError:
                        retry_count += 1
                        delay = base_delay**retry_count + extra_delay
                        print(
                            f"Rate limit hit. Retrying in {delay} seconds... (Attempt {retry_count}/{max_retries})"
                        )
                        time.sleep(delay)

                        if retry_count >= max_retries:
                            print("Max retries reached for rate limit. Crashing.")
                            raise RateLimitError(
                                "Rate limit exceeded after maximum retries", 429, {}
                            )

                    except Exception as e:
                        print(f"Unexpected error during pagination: {str(e)}")
                        break

                # If no data was fetched or no more pages, break the main loop
                if not data or cursor is None:
                    break

            print(f"Total {operation_name} fetched: {len(all_items)}")
            return all_items

        except Exception as e:
            self._handle_api_exceptions(e, operation_name)
            return None

    def _handle_http_response(
        self, response: requests.Response, operation: str
    ) -> Dict[str, Any]:
        """
        Handle HTTP response and raise appropriate exceptions.

        Args:
            response: The HTTP response object
            operation: Description of the operation

        Returns:
            JSON response data

        Raises:
            Various API exceptions based on status code
        """
        if response.status_code == 401:
            raise AuthenticationError(
                "Invalid API key", 401, response.json() if response.content else {}
            )
        elif response.status_code == 422:
            raise ValidationError(
                "Invalid request parameters",
                422,
                response.json() if response.content else {},
            )
        elif response.status_code == 404:
            raise NotFoundError(
                "Resource not found", 404, response.json() if response.content else {}
            )
        elif response.status_code == 429:
            raise RateLimitError(
                "Rate limit exceeded", 429, response.json() if response.content else {}
            )
        elif response.status_code >= 500:
            raise ServerError(
                "API server error",
                response.status_code,
                response.json() if response.content else {},
            )
        elif response.status_code != 200:
            raise BallDontLieException(
                f"HTTP {response.status_code}",
                response.status_code,
                response.json() if response.content else {},
            )

        return response.json()

    def get_teams(self) -> Optional[List[Any]]:
        """
        Retrieve all NBA teams from the API.

        Fetches the complete list of NBA teams from the Balldontlie API.

        Returns:
            List of team objects if successful, None if an error occurs
        """
        try:
            print("Getting teams...")
            return self.api.nba.teams.list().data
        except Exception as e:
            self._handle_api_exceptions(e, "teams retrieval")
            return None

    def get_players(self) -> Optional[List[Any]]:
        """
        Retrieve all NBA players from the API with pagination and rate limit handling.

        Fetches all NBA players from the Balldontlie API using pagination
        to handle large datasets. Implements retry logic with exponential backoff
        for rate limit handling.

        Returns:
            List of player objects if successful, None if an error occurs
        """
        return self._paginate_with_rate_limit(
            fetch_page=lambda **params: self.api.nba.players.list(**params),
            operation_name="players",
            per_page=25,
            max_retries=5,
            base_delay=2,
            extra_delay=0,
            page_delay=3,
        )

    def get_active_players(self) -> Optional[List[Any]]:
        """
        Retrieve all active NBA players from the API with pagination and rate limit handling.

        Fetches all active NBA players from the Balldontlie API using pagination
        to handle large datasets. Implements retry logic with exponential backoff
        for rate limit handling.

        Returns:
            List of active player objects if successful, None if an error occurs
        """
        return self._paginate_with_rate_limit(
            fetch_page=lambda **params: self.api.nba.players.list_active(**params),
            operation_name="active players",
            per_page=25,
            max_retries=5,
            base_delay=2,
            extra_delay=5,
            page_delay=3,
        )

    def get_injuries(self) -> Optional[List[Any]]:
        """
        Retrieve all NBA player injuries from the API with pagination and rate limit handling.

        Fetches all NBA player injuries from the Balldontlie API using pagination
        to handle large datasets. Implements retry logic with exponential backoff
        for rate limit handling.

        Returns:
            List of injury objects if successful, None if an error occurs
        """
        return self._paginate_with_rate_limit(
            fetch_page=lambda **params: self.api.nba.injuries.list(**params),
            operation_name="player injuries",
            per_page=25,
            max_retries=5,
            base_delay=2,
            extra_delay=5,
            page_delay=3,
        )

    def get_season_averages(
        self, category: str, season_type: str, type_param: str, season: int
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve NBA season averages from the API using direct HTTP requests.

        Fetches NBA season averages from the Balldontlie API using specified parameters.
        Uses direct HTTP requests since the SDK doesn't support the new category-based endpoints.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            category: Category type (general, clutch, defense, shooting)
            season_type: Season type (regular, playoffs, ist, playin)
            type_param: Type parameter (advanced, base, misc, scoring, usage, etc.)
            season: Season year (e.g., 2024)

        Returns:
            List of season averages dictionaries if successful, None if an error occurs
        """
        try:
            print(
                f"Getting season averages for category: {category}, season_type: {season_type}, type: {type_param}, season: {season}..."
            )

            # Get API key from environment
            api_key = os.getenv("BALLDONTLIE_API_KEY")
            if not api_key:
                raise ValueError("BALLDONTLIE_API_KEY environment variable is required")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            def fetch_season_averages_page(**params):
                base_url = (
                    f"https://api.balldontlie.io/nba/v1/season_averages/{category}"
                )
                request_params = {
                    "per_page": params.get("per_page", 25),
                    "season_type": season_type,
                    "type": type_param,
                    "season": season,
                }

                if "cursor" in params and params["cursor"] is not None:
                    request_params["cursor"] = params["cursor"]

                response = requests.get(
                    base_url, headers=headers, params=request_params, timeout=30
                )
                response_data = self._handle_http_response(response, "season averages")

                # Create a mock response object to match the expected interface
                class MockResponse:
                    def __init__(self, data, meta):
                        self.data = data
                        self.meta = type("Meta", (), meta)()

                return MockResponse(
                    data=response_data.get("data", []),
                    meta=response_data.get("meta", {}),
                )

            return self._paginate_with_rate_limit(
                fetch_page=fetch_season_averages_page,
                operation_name=f"season averages ({category}/{season_type}/{type_param})",
                per_page=25,
                max_retries=5,
                base_delay=3,
                extra_delay=8,
                page_delay=3,
            )

        except Exception as e:
            self._handle_api_exceptions(e, "season averages retrieval")
            return None

    def get_team_standings(self, season: int) -> Optional[List[Any]]:
        """
        Retrieve NBA team standings for a specific season from the API.

        Fetches all NBA team standings for the specified season from the Balldontlie API.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            season: Season year (e.g., 2024)

        Returns:
            List of team standings objects if successful, None if an error occurs
        """
        try:
            print(f"Getting team standings for season: {season}...")

            def fetch_standings():
                response = self.api.nba.standings.get(season=season)
                return response.data

            result = self._handle_rate_limit_with_retry(
                operation=fetch_standings, max_retries=5, base_delay=2, extra_delay=5
            )

            if result is not None:
                print(f"Fetched {len(result)} team standings for season {season}")
                return result
            else:
                print(f"No team standings found for season {season}")
                return []

        except Exception as e:
            self._handle_api_exceptions(e, "team standings retrieval")
            return None

    def get_games(self, game_date: date) -> Optional[List[Any]]:
        """
        Retrieve NBA games for a specific date from the API.

        Fetches all NBA games for the specified date from the Balldontlie API.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            game_date: Date object for which to fetch games

        Returns:
            List of game objects if successful, None if an error occurs
        """

        def fetch_games_page(**params):
            request_params = {
                "per_page": params.get("per_page", 25),
                "dates": [game_date.strftime("%Y-%m-%d")],
            }

            if "cursor" in params and params["cursor"] is not None:
                request_params["cursor"] = params["cursor"]

            return self.api.nba.games.list(**request_params)

        return self._paginate_with_rate_limit(
            fetch_page=fetch_games_page,
            operation_name=f"games for {game_date}",
            per_page=25,
            max_retries=5,
            base_delay=2,
            extra_delay=5,
            page_delay=2,
        )

    def get_stats(self, game_date: date) -> Optional[List[Any]]:
        """
        Retrieve NBA player stats for games on a specific date from the API.

        Fetches all NBA player stats for games on the specified date from the Balldontlie API.
        Implements retry logic with exponential backoff and longer delays for rate limit handling.

        Args:
            game_date: Date object for which to fetch player stats

        Returns:
            List of player stats objects if successful, None if an error occurs
        """

        def fetch_stats_page(**params):
            request_params = {
                "per_page": params.get("per_page", 25),
                "dates": [game_date.strftime("%Y-%m-%d")],
            }

            if "cursor" in params and params["cursor"] is not None:
                request_params["cursor"] = params["cursor"]

            return self.api.nba.stats.list(**request_params)

        return self._paginate_with_rate_limit(
            fetch_page=fetch_stats_page,
            operation_name=f"player stats for {game_date}",
            per_page=25,
            max_retries=5,
            base_delay=3,
            extra_delay=8,
            page_delay=3,
        )
