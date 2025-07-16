"""
The Odds API client library.

This module provides a wrapper around The Odds API for sports betting data retrieval.
"""

import requests
from dotenv import load_dotenv
import os
from typing import List, Optional, Any, Dict, Callable, TypeVar
import time
from datetime import date

load_dotenv()

T = TypeVar("T")


class TheOddsAPIException(Exception):
    """Base exception for The Odds API errors."""

    def __init__(self, message: str, status_code: int, response_data: Dict[str, Any]):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data
        super().__init__(self.message)


class AuthenticationError(TheOddsAPIException):
    """Raised when API key is invalid."""

    pass


class RateLimitError(TheOddsAPIException):
    """Raised when rate limit is exceeded."""

    pass


class ValidationError(TheOddsAPIException):
    """Raised when request parameters are invalid."""

    pass


class NotFoundError(TheOddsAPIException):
    """Raised when resource is not found."""

    pass


class ServerError(TheOddsAPIException):
    """Raised when server encounters an error."""

    pass


class TheOddsAPILib:
    """
    A wrapper class for The Odds API.

    This class provides methods to interact with The Odds API
    for retrieving sports betting data such as sports, odds, and events.
    """

    def __init__(self) -> None:
        """
        Initialize The Odds API client.

        Initializes the API client using the THE_ODDS_API_KEY
        environment variable.

        Raises:
            ValueError: If the API key is not found in environment variables
        """
        api_key = os.getenv("THEODDSAPI_API_KEY")
        if not api_key:
            raise ValueError("THEODDSAPI_API_KEY environment variable is required")

        self.api_key = api_key
        self.base_url = "https://api.the-odds-api.com/v4"
        self.session = requests.Session()

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
        elif isinstance(e, TheOddsAPIException):
            print(
                f"General API error during {operation}. Status: {e.status_code}, Details: {e.response_data}"
            )
        else:
            print(f"Unexpected error during {operation}: {str(e)}")

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
                "Server error",
                response.status_code,
                response.json() if response.content else {},
            )
        elif response.status_code != 200:
            raise TheOddsAPIException(
                f"HTTP {response.status_code}",
                response.status_code,
                response.json() if response.content else {},
            )

        return response.json()

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        operation: str = "API request",
    ) -> Dict[str, Any]:
        """
        Make a request to The Odds API.

        Args:
            endpoint: The API endpoint to call
            params: Query parameters for the request
            operation: Description of the operation for error handling

        Returns:
            JSON response data

        Raises:
            Various API exceptions based on response
        """
        url = f"{self.base_url}{endpoint}"

        # Always include API key
        request_params = {"apiKey": self.api_key}
        if params:
            request_params.update(params)

        try:
            response = self.session.get(url, params=request_params)
            return self._handle_http_response(response, operation)
        except requests.exceptions.RequestException as e:
            print(f"Request failed during {operation}: {str(e)}")
            raise TheOddsAPIException(f"Request failed: {str(e)}", 0, {})

    def get_sports(self, all_sports: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Get a list of available sports.

        Args:
            all_sports: If True, returns both in-season and out-of-season sports.
                       If False, returns only in-season sports.

        Returns:
            List of sports data if successful, None if failed
        """
        try:
            print("Getting sports...")

            params = {}
            if all_sports:
                params["all"] = "true"

            sports_data = self._make_request("/sports/", params, "get_sports")

            print(f"Successfully fetched {len(sports_data)} sports")
            return sports_data

        except Exception as e:
            self._handle_api_exceptions(e, "get_sports")
            return None

    def get_odds(
        self,
        sport: str,
        regions: str = "us",
        markets: Optional[str] = None,
        date_format: str = "iso",
        odds_format: str = "decimal",
        event_ids: Optional[str] = None,
        bookmakers: Optional[str] = None,
        commence_time_from: Optional[str] = None,
        commence_time_to: Optional[str] = None,
        include_links: bool = False,
        include_sids: bool = False,
        include_bet_limits: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get odds for a specific sport.

        Args:
            sport: The sport key (e.g., 'basketball_nba', 'upcoming')
            regions: Comma-separated regions (us, uk, au, eu). Defaults to 'us'
            markets: Comma-separated markets (h2h, spreads, totals, outrights). Defaults to None (h2h)
            date_format: Format for timestamps ('unix' or 'iso'). Defaults to 'iso'
            odds_format: Format for odds ('decimal' or 'american'). Defaults to 'decimal'
            event_ids: Comma-separated event IDs to filter. Defaults to None
            bookmakers: Comma-separated bookmaker keys. Defaults to None
            commence_time_from: Filter games from this time (ISO 8601). Defaults to None
            commence_time_to: Filter games until this time (ISO 8601). Defaults to None
            include_links: Include bookmaker links. Defaults to False
            include_sids: Include source IDs. Defaults to False
            include_bet_limits: Include bet limits. Defaults to False

        Returns:
            List of odds data if successful, None if failed
        """
        try:
            print(f"Getting odds for sport: {sport}")

            params = {
                "regions": regions,
                "dateFormat": date_format,
                "oddsFormat": odds_format,
            }

            if markets:
                params["markets"] = markets
            if event_ids:
                params["eventIds"] = event_ids
            if bookmakers:
                params["bookmakers"] = bookmakers
            if commence_time_from:
                params["commenceTimeFrom"] = commence_time_from
            if commence_time_to:
                params["commenceTimeTo"] = commence_time_to
            if include_links:
                params["includeLinks"] = "true"
            if include_sids:
                params["includeSids"] = "true"
            if include_bet_limits:
                params["includeBetLimits"] = "true"

            odds_data = self._make_request(f"/sports/{sport}/odds/", params, "get_odds")

            print(f"Successfully fetched {len(odds_data)} odds events")
            return odds_data

        except Exception as e:
            self._handle_api_exceptions(e, "get_odds")
            return None

    def get_participants(self, sport: str) -> Optional[List[Dict[str, Any]]]:
        """
        Get participants (teams/players) for a specific sport.

        Args:
            sport: The sport key (e.g., 'basketball_nba', 'americanfootball_nfl')

        Returns:
            List of participants data if successful, None if failed
        """
        try:
            print(f"Getting participants for sport: {sport}")

            participants_data = self._make_request(
                f"/sports/{sport}/participants", {}, "get_participants"
            )

            print(f"Successfully fetched {len(participants_data)} participants")
            return participants_data

        except Exception as e:
            self._handle_api_exceptions(e, "get_participants")
            return None

    def get_historical_odds(
        self,
        sport: str,
        date: str,
        regions: str = "us",
        markets: Optional[str] = None,
        date_format: str = "iso",
        odds_format: str = "decimal",
        event_ids: Optional[str] = None,
        bookmakers: Optional[str] = None,
        commence_time_from: Optional[str] = None,
        commence_time_to: Optional[str] = None,
        include_links: bool = False,
        include_sids: bool = False,
        include_bet_limits: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """
        Get historical odds for a specific sport at a given timestamp.

        Args:
            sport: The sport key (e.g., 'basketball_nba', 'americanfootball_nfl')
            date: The timestamp in ISO8601 format (e.g., '2021-10-18T12:00:00Z')
            regions: Comma-separated regions (us, uk, au, eu). Defaults to 'us'
            markets: Comma-separated markets (h2h, spreads, totals, outrights). Defaults to None (h2h)
            date_format: Format for timestamps ('unix' or 'iso'). Defaults to 'iso'
            odds_format: Format for odds ('decimal' or 'american'). Defaults to 'decimal'
            event_ids: Comma-separated event IDs to filter. Defaults to None
            bookmakers: Comma-separated bookmaker keys. Defaults to None
            commence_time_from: Filter games from this time (ISO 8601). Defaults to None
            commence_time_to: Filter games until this time (ISO 8601). Defaults to None
            include_links: Include bookmaker links. Defaults to False
            include_sids: Include source IDs. Defaults to False
            include_bet_limits: Include bet limits. Defaults to False

        Returns:
            Historical odds data with timestamp info if successful, None if failed

        Note:
            This endpoint costs 10 credits per market per region and is only available on paid plans.
        """
        try:
            print(f"Getting historical odds for sport: {sport} at {date}")

            params = {
                "regions": regions,
                "date": date,
                "dateFormat": date_format,
                "oddsFormat": odds_format,
            }

            if markets:
                params["markets"] = markets
            if event_ids:
                params["eventIds"] = event_ids
            if bookmakers:
                params["bookmakers"] = bookmakers
            if commence_time_from:
                params["commenceTimeFrom"] = commence_time_from
            if commence_time_to:
                params["commenceTimeTo"] = commence_time_to
            if include_links:
                params["includeLinks"] = "true"
            if include_sids:
                params["includeSids"] = "true"
            if include_bet_limits:
                params["includeBetLimits"] = "true"

            historical_data = self._make_request(
                f"/historical/sports/{sport}/odds", params, "get_historical_odds"
            )

            print(f"Successfully fetched historical odds data")
            print(f"Snapshot timestamp: {historical_data.get('timestamp')}")
            print(f"Data points: {len(historical_data.get('data', []))}")

            return historical_data

        except Exception as e:
            self._handle_api_exceptions(e, "get_historical_odds")
            return None

    def get_historical_events(
        self,
        sport: str,
        date: str,
        date_format: str = "iso",
        event_ids: Optional[str] = None,
        commence_time_from: Optional[str] = None,
        commence_time_to: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get historical events for a specific sport at a given timestamp.

        Args:
            sport: The sport key (e.g., 'basketball_nba', 'americanfootball_nfl')
            date: The timestamp in ISO8601 format (e.g., '2021-10-18T12:00:00Z')
            date_format: Format for timestamps ('unix' or 'iso'). Defaults to 'iso'
            event_ids: Comma-separated event IDs to filter. Defaults to None
            commence_time_from: Filter games from this time (ISO 8601). Defaults to None
            commence_time_to: Filter games until this time (ISO 8601). Defaults to None

        Returns:
            Historical events data with timestamp info if successful, None if failed

        Note:
            This endpoint costs 1 credit and is only available on paid plans.
        """
        try:
            print(f"Getting historical events for sport: {sport} at {date}")

            params = {
                "date": date,
                "dateFormat": date_format,
            }

            if event_ids:
                params["eventIds"] = event_ids
            if commence_time_from:
                params["commenceTimeFrom"] = commence_time_from
            if commence_time_to:
                params["commenceTimeTo"] = commence_time_to

            historical_events = self._make_request(
                f"/historical/sports/{sport}/events", params, "get_historical_events"
            )

            print(f"Successfully fetched historical events data")
            print(f"Snapshot timestamp: {historical_events.get('timestamp')}")
            print(f"Events found: {len(historical_events.get('data', []))}")

            return historical_events

        except Exception as e:
            self._handle_api_exceptions(e, "get_historical_events")
            return None

    def get_historical_event_odds(
        self,
        sport: str,
        event_id: str,
        date: str,
        regions: str = "us",
        markets: Optional[str] = None,
        date_format: str = "iso",
        odds_format: str = "decimal",
        bookmakers: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Get historical odds for a specific event at a given timestamp.

        Args:
            sport: The sport key (e.g., 'basketball_nba', 'americanfootball_nfl')
            event_id: The specific event ID to get odds for
            date: The timestamp in ISO8601 format (e.g., '2021-10-18T12:00:00Z')
            regions: Comma-separated regions (us, uk, au, eu). Defaults to 'us'
            markets: Comma-separated markets. Defaults to None
            date_format: Format for timestamps ('unix' or 'iso'). Defaults to 'iso'
            odds_format: Format for odds ('decimal' or 'american'). Defaults to 'decimal'
            bookmakers: Comma-separated bookmaker keys. Defaults to None

        Returns:
            Historical odds data for the specific event if successful, None if failed

        Note:
            This endpoint costs 10 credits per market per region and is only available on paid plans.
        """
        try:
            print(f"Getting historical odds for event {event_id} at {date}")

            params = {
                "regions": regions,
                "date": date,
                "dateFormat": date_format,
                "oddsFormat": odds_format,
            }

            if markets:
                params["markets"] = markets
            if bookmakers:
                params["bookmakers"] = bookmakers

            historical_odds = self._make_request(
                f"/historical/sports/{sport}/events/{event_id}/odds",
                params,
                "get_historical_event_odds",
            )

            print(f"Successfully fetched historical odds for event {event_id}")
            print(f"Snapshot timestamp: {historical_odds.get('timestamp')}")

            return historical_odds

        except Exception as e:
            self._handle_api_exceptions(e, "get_historical_event_odds")
            return None
