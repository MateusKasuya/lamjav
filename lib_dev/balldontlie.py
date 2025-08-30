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
from datetime import date, timedelta
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
                        # Add more detailed error information for debugging
                        if "HTTP 400" in str(e):
                            print(
                                "HTTP 400 Bad Request - This usually means invalid API parameters"
                            )
                            print(
                                "Check the Balldontlie API documentation for valid parameter combinations"
                            )
                        elif "HTTP 422" in str(e):
                            print(
                                "HTTP 422 Validation Error - Check parameter values and formats"
                            )
                        elif "HTTP 502" in str(e) or "Bad Gateway" in str(e):
                            print(
                                "HTTP 502 Bad Gateway - Server is experiencing issues"
                            )
                            print("This is usually a temporary server-side problem")
                            print("Consider waiting and retrying later")
                        elif "HTTP 503" in str(e):
                            print(
                                "HTTP 503 Service Unavailable - Server is temporarily unavailable"
                            )
                        elif "HTTP 504" in str(e):
                            print(
                                "HTTP 504 Gateway Timeout - Server took too long to respond"
                            )
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
            base_delay=5,
            extra_delay=10,
            page_delay=5,
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
            base_delay=5,
            extra_delay=15,
            page_delay=5,
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
            base_delay=5,
            extra_delay=15,
            page_delay=5,
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
                base_delay=8,
                extra_delay=20,
                page_delay=8,
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
                operation=fetch_standings, max_retries=5, base_delay=5, extra_delay=15
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
            base_delay=8,
            extra_delay=20,
            page_delay=5,
        )

    def get_games_by_season(self, season: int) -> Optional[List[Any]]:
        """
        Retrieve all NBA games for a specific season from the API.

        Fetches all NBA games for the specified season from the Balldontlie API.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            season: Season year (e.g., 2024)

        Returns:
            List of game objects if successful, None if an error occurs
        """

        def fetch_games_page(**params):
            request_params = {
                "per_page": params.get("per_page", 25),
                "seasons": [season],
            }

            if "cursor" in params and params["cursor"] is not None:
                request_params["cursor"] = params["cursor"]

            return self.api.nba.games.list(**request_params)

        return self._paginate_with_rate_limit(
            fetch_page=fetch_games_page,
            operation_name=f"games for season {season}",
            per_page=25,
            max_retries=5,
            base_delay=8,
            extra_delay=20,
            page_delay=5,
        )

    def get_games_with_datetime(
        self, game_date: date
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve NBA games for a specific date using direct requests to preserve datetime field.

        This method bypasses the SDK to ensure all fields, including datetime, are preserved
        in the raw JSON response before processing.

        Args:
            game_date: Date object for which to fetch games

        Returns:
            List of game dictionaries with datetime preserved if successful, None if an error occurs
        """
        try:
            print(f"Getting games with datetime preservation for {game_date}...")

            # Get API key from environment
            api_key = os.getenv("BALLDONTLIE_API_KEY")
            if not api_key:
                raise ValueError("BALLDONTLIE_API_KEY environment variable is required")

            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }

            params = {
                "dates[]": game_date.strftime("%Y-%m-%d"),
                "per_page": 100,  # Get more games per page
            }

            all_games = []
            cursor = None

            while True:
                if cursor:
                    params["cursor"] = cursor

                response = requests.get(
                    "https://api.balldontlie.io/v1/games",
                    headers=headers,
                    params=params,
                    timeout=30,
                )

                if response.status_code != 200:
                    print(f"Error fetching games: {response.status_code}")
                    break

                data = response.json()
                games = data.get("data", [])

                if not games:
                    break

                all_games.extend(games)
                print(f"Fetched {len(games)} games. Total: {len(all_games)}")

                # Check for next page
                meta = data.get("meta", {})
                cursor = meta.get("next_cursor")

                if not cursor:
                    break

                # Rate limiting
                time.sleep(1)

            if all_games:
                # Verify datetime field is present
                games_with_datetime = [g for g in all_games if g.get("datetime")]
                games_without_datetime = [g for g in all_games if not g.get("datetime")]

                print(f"  Games with datetime: {len(games_with_datetime)}")
                print(f"  Games without datetime: {len(games_without_datetime)}")

                if games_with_datetime:
                    # Show example of datetime field
                    example_game = games_with_datetime[0]
                    print(f"  Example datetime: {example_game.get('datetime')}")

                print(
                    f"Total games fetched with datetime preservation: {len(all_games)}"
                )
                return all_games
            else:
                print(f"No games found for {game_date}")
                return []

        except Exception as e:
            self._handle_api_exceptions(
                e, f"games with datetime retrieval for {game_date}"
            )
            return None

    def get_games_by_date_range(
        self, start_date: date, end_date: date
    ) -> Optional[List[Any]]:
        """
        Retrieve NBA games for a date range from the API.

        Fetches all NBA games between the specified start and end dates from the Balldontlie API.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            start_date: Start date for the range
            end_date: End date for the range

        Returns:
            List of game objects if successful, None if an error occurs
        """

        def fetch_games_page(**params):
            request_params = {
                "per_page": params.get("per_page", 25),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
            }

            if "cursor" in params and params["cursor"] is not None:
                request_params["cursor"] = params["cursor"]

            return self.api.nba.games.list(**request_params)

        return self._paginate_with_rate_limit(
            fetch_page=fetch_games_page,
            operation_name=f"games from {start_date} to {end_date}",
            per_page=25,
            max_retries=5,
            base_delay=8,
            extra_delay=20,
            page_delay=5,
        )

    def get_games_by_date_range_with_datetime(
        self, start_date: date, end_date: date
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Retrieve NBA games for a date range using direct requests to preserve datetime field.

        This method bypasses the SDK to ensure all fields, including datetime, are preserved
        in the raw JSON response before processing.

        Args:
            start_date: Start date for the range
            end_date: End date for the range

        Returns:
            List of game dictionaries with datetime preserved if successful, None if an error occurs
        """
        try:
            print(
                f"Getting games with datetime preservation from {start_date} to {end_date}..."
            )

            all_games = []
            current_date = start_date

            while current_date <= end_date:
                print(f"Processing games for date: {current_date}")

                games = self.get_games_with_datetime(current_date)
                if games:
                    all_games.extend(games)
                    print(
                        f"Added {len(games)} games for {current_date}. Total: {len(all_games)}"
                    )

                # Move to next date
                current_date += timedelta(days=1)

                # Rate limiting between dates
                time.sleep(1)

            print(f"Total games fetched with datetime preservation: {len(all_games)}")
            return all_games

        except Exception as e:
            self._handle_api_exceptions(
                e, f"games with datetime retrieval from {start_date} to {end_date}"
            )
            return None

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
            base_delay=8,
            extra_delay=20,
            page_delay=8,
        )

    def get_advanced_stats(
        self,
        player_ids: Optional[List[int]] = None,
        game_ids: Optional[List[int]] = None,
        dates: Optional[List[date]] = None,
        seasons: Optional[List[int]] = None,
        postseason: Optional[bool] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        per_page: int = 25,
    ) -> Optional[List[Any]]:
        """
        Retrieve NBA advanced stats from the API with flexible filtering options.

        Fetches NBA advanced stats from the Balldontlie API using various filter parameters.
        Implements retry logic with exponential backoff for rate limit handling.

        Args:
            player_ids: List of player IDs to filter by
            game_ids: List of game IDs to filter by
            dates: List of specific dates to filter by (YYYY-MM-DD format)
            seasons: List of seasons to filter by (e.g., [2022, 2023])
            postseason: Filter by postseason (True for playoffs, False for regular season, None for both)
            start_date: Start date for date range filtering
            end_date: End date for date range filtering
            per_page: Number of results per page (max 100)

        Returns:
            List of advanced stats objects if successful, None if an error occurs
        """
        try:
            print("Getting advanced stats...")

            # Build filter parameters
            filters = []
            if player_ids:
                filters.append(f"player_ids: {player_ids}")
            if game_ids:
                filters.append(f"game_ids: {game_ids}")
            if dates:
                filters.append(f"dates: {[d.strftime('%Y-%m-%d') for d in dates]}")
            if seasons:
                filters.append(f"seasons: {seasons}")
            if postseason is not None:
                filters.append(f"postseason: {postseason}")
            if start_date:
                filters.append(f"start_date: {start_date.strftime('%Y-%m-%d')}")
            if end_date:
                filters.append(f"end_date: {end_date.strftime('%Y-%m-%d')}")

            if filters:
                print(f"Applying filters: {', '.join(filters)}")

            def fetch_advanced_stats_page(**params):
                request_params = {
                    "per_page": params.get("per_page", per_page),
                }

                # Add cursor for pagination
                if "cursor" in params and params["cursor"] is not None:
                    request_params["cursor"] = params["cursor"]

                # Add player_ids filter - SDK expects list directly
                if player_ids:
                    request_params["player_ids"] = player_ids

                # Add game_ids filter - SDK expects list directly
                if game_ids:
                    request_params["game_ids"] = game_ids

                # Add dates filter - SDK expects list directly
                if dates:
                    request_params["dates"] = [d.strftime("%Y-%m-%d") for d in dates]

                # Add seasons filter - SDK expects list directly
                if seasons:
                    request_params["seasons"] = seasons

                # Add postseason filter
                if postseason is not None:
                    request_params["postseason"] = postseason

                # Add date range filters
                if start_date:
                    request_params["start_date"] = start_date.strftime("%Y-%m-%d")
                if end_date:
                    request_params["end_date"] = end_date.strftime("%Y-%m-%d")

                return self.api.nba.advanced_stats.list(**request_params)

            return self._paginate_with_rate_limit(
                fetch_page=fetch_advanced_stats_page,
                operation_name="advanced stats",
                per_page=per_page,
                max_retries=5,
                base_delay=8,
                extra_delay=20,
                page_delay=8,
            )

        except Exception as e:
            self._handle_api_exceptions(e, "advanced stats retrieval")
            return None

    def get_leaders(
        self,
        stat_type: str,
        season: int,
    ) -> Optional[List[Any]]:
        """
        Retrieve NBA leaders for a specific stat type and season from the API.

        Fetches NBA leaders data from the Balldontlie API for the specified stat type
        and season. Implements retry logic with exponential backoff for rate limit handling.

        Args:
            stat_type: Type of stat to get leaders for (reb, dreb, tov, ast, oreb, min, pts, stl, blk)
            season: Season year (e.g., 2023)

        Returns:
            List of leader objects if successful, None if an error occurs
        """
        try:
            print(
                f"Getting NBA leaders for stat_type: {stat_type}, season: {season}..."
            )

            def fetch_leaders():
                response = self.api.nba.leaders.get(stat_type=stat_type, season=season)
                return response.data

            result = self._handle_rate_limit_with_retry(
                operation=fetch_leaders, max_retries=5, base_delay=5, extra_delay=15
            )

            if result is not None:
                print(
                    f"Fetched {len(result)} leaders for {stat_type} in season {season}"
                )
                return result
            else:
                print(f"No leaders found for {stat_type} in season {season}")
                return []

        except Exception as e:
            self._handle_api_exceptions(e, "leaders retrieval")
            return None


class SeasonAveragesProcessor:
    """
    A specialized class for processing NBA season averages data.

    This class encapsulates all the logic for fetching, processing, and organizing
    season averages data from the Balldontlie API.
    """

    def __init__(self, balldontlie_client: BalldontlieLib, smartbetting_client: Any):
        """
        Initialize the SeasonAveragesProcessor.

        Args:
            balldontlie_client: Initialized BalldontlieLib client
            smartbetting_client: Initialized SmartbettingLib client
        """
        self.balldontlie = balldontlie_client
        self.smartbetting = smartbetting_client

    def get_all_combinations(self) -> List[tuple]:
        """
        Get all valid category/type combinations for season averages.

        Returns:
            List of tuples containing (category, type) combinations
        """
        combinations = [
            # General category
            ("general", "base"),
            ("general", "advanced"),
            ("general", "usage"),
            ("general", "scoring"),
            ("general", "defense"),
            ("general", "misc"),
            # Clutch category
            ("clutch", "advanced"),
            ("clutch", "base"),
            ("clutch", "misc"),
            ("clutch", "scoring"),
            ("clutch", "usage"),
            # Defense category
            ("defense", "2_pointers"),
            ("defense", "3_pointers"),
            ("defense", "greater_than_15ft"),
            ("defense", "less_than_10ft"),
            ("defense", "less_than_6ft"),
            ("defense", "overall"),
            # Shooting category
            ("shooting", "5ft_range"),
            ("shooting", "by_zone"),
        ]

        return combinations

    def get_season_types_for_category(self, category: str) -> List[str]:
        """
        Get the appropriate season types for a specific category.

        Args:
            category: Category name (general, clutch, defense, shooting)

        Returns:
            List of season types to process for this category
        """
        if category == "general":
            return ["regular", "playoffs", "ist", "playin"]
        elif category in ["clutch", "defense", "shooting"]:
            return ["regular", "playoffs", "ist"]
        else:
            return ["regular", "playoffs", "ist"]

    def fetch_and_upload_season_averages(
        self,
        bucket: str,
        category: str,
        type_param: str,
        season_type: str,
        season: int,
        extraction_date: str,
    ) -> bool:
        """
        Fetch season averages for a specific combination and upload to GCS.

        Args:
            bucket: GCS bucket name
            category: Season averages category
            type_param: Season averages type
            season_type: Season type
            season: Season year
            extraction_date: Date of extraction

        Returns:
            True if successful, False otherwise
        """
        try:
            print(
                f"Processing: category={category}, type={type_param}, season_type={season_type}, season={season}"
            )

            # Fetch season averages data from API
            response = self.balldontlie.get_season_averages(
                category, season_type, type_param, season
            )

            if response is None or len(response) == 0:
                print(
                    f"No data received for {category}/{type_param}/{season_type}/{season}"
                )
                return False

            # Convert API response to dictionary format (if needed)
            if response and hasattr(response[0], "model_dump"):
                data = self.smartbetting.convert_object_to_dict(response)
            else:
                data = response  # Already in dictionary format

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = self.smartbetting.convert_to_ndjson(data)

            # Generate storage path and blob name
            storage_path = self._get_storage_path(
                category, type_param, season_type, season, extraction_date
            )
            gcs_blob_name = f"{storage_path}/season_averages_{category}_{type_param}_{season_type}_{season}.json"

            # Upload NDJSON data to Google Cloud Storage
            self.smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully uploaded {len(data)} records for {category}/{type_param}/{season_type}/{season}"
            )
            return True

        except Exception as e:
            print(
                f"❌ Error processing {category}/{type_param}/{season_type}/{season}: {str(e)}"
            )
            return False

    def _get_storage_path(
        self,
        category: str,
        type_param: str,
        season_type: str,
        season: int,
        extraction_date: str,
    ) -> str:
        """
        Generate the storage path for season averages data.

        Args:
            category: Season averages category (e.g., 'general', 'clutch', 'defense', 'shooting')
            type_param: Season averages type (e.g., 'base', 'advanced', 'usage')
            season_type: Season type (e.g., 'regular', 'playoffs', 'ist', 'playin')
            season: Season year
            extraction_date: Date of extraction (kept for compatibility but not used in path)

        Returns:
            Storage path string
        """
        from .utils import Catalog

        return f"{Catalog.NBA}/season_averages/{category}/{type_param}/{season_type}/{season}"

    def process_combinations(
        self,
        combinations: List[tuple],
        bucket: str,
        season: int,
        extraction_date: str,
        season_types: List[str] = None,
    ) -> tuple[int, int]:
        """
        Process a list of combinations and return success/failure counts.

        Args:
            combinations: List of (category, type_param) tuples
            bucket: GCS bucket name
            season: Season year
            extraction_date: Date of extraction
            season_types: List of season types to process (default: ['regular', 'playoffs'])

        Returns:
            Tuple of (successful_count, failed_count)
        """
        if season_types is None:
            season_types = ["regular", "playoffs"]

        successful_extractions = 0
        failed_extractions = 0

        total_combinations = len(combinations) * len(season_types)
        current_combination = 0

        for category, type_param in combinations:
            for season_type in season_types:
                current_combination += 1
                print(
                    f"\n[{current_combination}/{total_combinations}] Processing combination..."
                )

                success = self.fetch_and_upload_season_averages(
                    bucket=bucket,
                    category=category,
                    type_param=type_param,
                    season_type=season_type,
                    season=season,
                    extraction_date=extraction_date,
                )

                if success:
                    successful_extractions += 1
                else:
                    failed_extractions += 1

                # Add delay between API calls to respect rate limits
                if (
                    current_combination < total_combinations
                ):  # Don't sleep after the last call
                    time.sleep(2)

        return successful_extractions, failed_extractions

    def process_category_combinations(
        self,
        category: str,
        combinations: List[tuple],
        bucket: str,
        season: int,
        extraction_date: str,
    ) -> tuple[int, int]:
        """
        Process all combinations for a specific category.

        Args:
            category: Category name
            combinations: List of (category, type) combinations for this category
            bucket: GCS bucket name
            season: Season year
            extraction_date: Date of extraction

        Returns:
            Tuple of (successful_count, failed_count)
        """
        print(f"\n{'=' * 80}")
        print(f"PROCESSING {category.upper()} CATEGORY")
        print(f"{'=' * 80}")

        # Filter combinations for this category
        category_combinations = [
            (cat, type_param) for cat, type_param in combinations if cat == category
        ]

        # Get season types for this category
        season_types = self.get_season_types_for_category(category)

        print(f"Category: {category}")
        print(f"Types: {[type_param for _, type_param in category_combinations]}")
        print(f"Season Types: {season_types}")
        print(f"Total combinations: {len(category_combinations) * len(season_types)}")

        return self.process_combinations(
            combinations=category_combinations,
            bucket=bucket,
            season=season,
            extraction_date=extraction_date,
            season_types=season_types,
        )
