"""
Extract Historical Event Odds from The Odds API.

This script reads event IDs and commence times from the extract_event_ids.py output
and fetches historical odds for each event from The Odds API, then saves them
to Google Cloud Storage in the odds/landing/historical_event_odds folder.
"""

import sys
import os
from typing import List, Dict, Any, Optional
from datetime import date, datetime, timedelta
import json
import time

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


class HistoricalEventOddsExtractor:
    """
    Class to extract historical odds for specific events from The Odds API.
    """

    def __init__(self):
        """Initialize the extractor with API clients."""
        self.theoddsapi = TheOddsAPILib()
        self.smartbetting = SmartbettingLib()
        self.bucket = Bucket.LAMJAV_STORAGE
        self.catalog = Catalog.ODDS
        self.schema = Schema.LANDING
        self.table = "historical_event_odds"

        # Markets to extract (as specified in requirements)
        self.markets = [
            "player_points",
            "player_points_q1",
            "player_rebounds",
            "player_rebounds_q1",
            "player_assists",
            "player_assists_q1",
            "player_threes",
            "player_blocks",
            "player_steals",
            "player_blocks_steals",
            "player_turnovers",
            "player_points_rebounds_assists",
            "player_points_rebounds",
            "player_points_assists",
            "player_rebounds_assists",
            "player_field_goals",
            "player_frees_made",
            "player_frees_attempts",
            "player_first_basket",
            "player_first_team_basket",
            "player_double_double",
            "player_triple_double",
            "player_method_of_first_basket",
        ]

        # API configuration
        self.sport = "basketball_nba"
        self.regions = "us"
        self.odds_format = "decimal"

    def read_event_data_from_storage(self, target_date: date = None) -> Dict[str, str]:
        """
        Read event data from the extract_event_ids.py output.

        Args:
            target_date: Date of the file to read (defaults to today)

        Returns:
            Dictionary mapping event ID to commence time
        """
        if target_date is None:
            target_date = date.today()

        # Create filename: historical_event_id_YYYY-MM-DD.json
        filename = f"historical_event_id_{target_date.strftime('%Y-%m-%d')}.json"
        gcs_path = f"{self.catalog}/etl/historical_event_id/{filename}"

        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(str(self.bucket))
            blob = bucket.blob(gcs_path)

            # Download and parse JSON content
            content = blob.download_as_text()
            data = json.loads(content)

            # Extract events from the data structure
            events = data.get("events", [])
            event_data = {}

            for event in events:
                event_id = event.get("id")
                commence_time = event.get("commence_time")
                if event_id and commence_time:
                    event_data[event_id] = commence_time

            print(f"Read {len(event_data)} events from {gcs_path}")
            return event_data

        except Exception as e:
            print(f"Error reading event data from {gcs_path}: {e}")
            return {}

    def calculate_historical_date(
        self, commence_time: str, hours_before: int = 2
    ) -> str:
        """
        Calculate the historical date to query based on commence time.

        Args:
            commence_time: The commence time of the event
            hours_before: Hours before the event to query (default: 2)

        Returns:
            ISO8601 formatted date string for the API query
        """
        try:
            # Parse commence time
            commence_dt = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))

            # Calculate historical date (hours before commence time)
            historical_dt = commence_dt - timedelta(hours=hours_before)

            # Format as ISO8601
            return historical_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        except Exception as e:
            print(f"Error calculating historical date for {commence_time}: {e}")
            # Fallback: use commence time minus 2 hours
            return commence_time

    def fetch_historical_odds_for_event(
        self, event_id: str, historical_date: str
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch historical odds for a specific event.

        Args:
            event_id: The event ID to fetch odds for
            historical_date: The historical date to query

        Returns:
            Historical odds data if successful, None if failed
        """
        try:
            # Join markets with comma
            markets_str = ",".join(self.markets)

            # Fetch historical odds
            odds_data = self.theoddsapi.get_historical_event_odds(
                sport=self.sport,
                event_id=event_id,
                date=historical_date,
                regions=self.regions,
                markets=markets_str,
                odds_format=self.odds_format,
                bookmakers="fanduel",  # Use only FanDuel
            )

            return odds_data

        except Exception as e:
            print(f"Error fetching odds for event {event_id}: {e}")
            return None

    def save_event_odds_to_storage(
        self, event_id: str, odds_data: Dict[str, Any], historical_date: str
    ) -> str:
        """
        Save historical odds data for a specific event to GCS.

        Args:
            event_id: The event ID
            odds_data: The odds data to save
            historical_date: The historical date used for the query

        Returns:
            The GCS blob name where the file was saved
        """
        # Create filename: historical_event_odds_{event_id}_{date}.json
        date_str = historical_date.split("T")[0]  # Extract date part
        filename = f"historical_event_odds_{event_id}_{date_str}.json"
        gcs_path = f"{self.catalog}/{self.schema}/{self.table}/{filename}"

        try:
            # Convert data to JSON format
            json_data = self.smartbetting.convert_to_json(odds_data)

            # Upload to GCS
            self.smartbetting.upload_json_to_gcs(json_data, self.bucket, gcs_path)

            print(f"✅ Saved odds for event {event_id} to {gcs_path}")
            return gcs_path

        except Exception as e:
            print(f"❌ Error saving odds for event {event_id}: {e}")
            return None

    def extract_and_save_historical_odds(
        self, target_date: date = None, max_events: int = None, delay_seconds: int = 1
    ) -> Dict[str, str]:
        """
        Extract and save historical odds for all events.

        Args:
            target_date: Date of the event data file to read
            max_events: Maximum number of events to process (for testing)
            delay_seconds: Delay between API calls to avoid rate limiting

        Returns:
            Dictionary mapping event ID to saved file path
        """
        print("Starting historical event odds extraction...")

        # Read event data
        event_data = self.read_event_data_from_storage(target_date)
        if not event_data:
            print("No event data found")
            return {}

        # Limit events if specified
        if max_events:
            event_items = list(event_data.items())[:max_events]
            event_data = dict(event_items)
            print(f"Limited to {max_events} events for testing")

        print(f"Processing {len(event_data)} events...")

        saved_files = {}
        processed = 0
        failed = 0

        for event_id, commence_time in event_data.items():
            try:
                processed += 1
                print(f"Processing event {processed}/{len(event_data)}: {event_id}")

                # Calculate historical date
                historical_date = self.calculate_historical_date(commence_time)

                # Fetch historical odds
                odds_data = self.fetch_historical_odds_for_event(
                    event_id, historical_date
                )

                if odds_data:
                    # Save to storage
                    saved_path = self.save_event_odds_to_storage(
                        event_id, odds_data, historical_date
                    )
                    if saved_path:
                        saved_files[event_id] = saved_path
                else:
                    failed += 1
                    print(f"⚠️  No odds data for event {event_id}")

                # Delay between requests to avoid rate limiting
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            except Exception as e:
                failed += 1
                print(f"❌ Error processing event {event_id}: {e}")
                continue

        print(f"\n✅ Extraction completed:")
        print(f"   Processed: {processed}")
        print(f"   Saved: {len(saved_files)}")
        print(f"   Failed: {failed}")

        return saved_files


def main():
    """
    Main function to extract and save historical event odds.
    """
    extractor = HistoricalEventOddsExtractor()

    # Extract odds for events from today's file
    # Full extraction without limits
    saved_files = extractor.extract_and_save_historical_odds(
        target_date=date.today(),
        delay_seconds=2,  # 2 second delay between API calls
    )

    if saved_files:
        print(f"\n✅ Successfully saved {len(saved_files)} event odds files")
    else:
        print("❌ No event odds files were saved")


if __name__ == "__main__":
    main()
