"""
Extract Historical Event Odds from The Odds API.

This script reads event IDs and commence times from the extract_event_ids.py output
and fetches historical odds for each event from The Odds API, then saves them
to Google Cloud Storage in the odds/landing/historical_event_odds folder.
"""

import sys
import os
from typing import Dict, Any, Optional
from datetime import date, datetime, timedelta
import time

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Season, Table

# No more external imports needed - using SmartbettingLib directly!


class HistoricalEventOddsExtractor:
    """
    Class to extract historical odds for specific events from The Odds API.
    """

    def __init__(self):
        """Initialize the extractor with API clients."""
        self.theoddsapi = TheOddsAPILib()
        self.smartbetting = SmartbettingLib()
        self.bucket = Bucket.SMARTBETTING_STORAGE  # Aligned with extract_event_ids.py
        self.catalog = Catalog.ODDS
        self.season = Season.SEASON_2024  # Aligned with extract_event_ids.py
        self.table = Table.HISTORICAL_EVENT_ODDS

        # Input configuration (from extract_event_ids.py)
        self.input_table = Table.HISTORICAL_EVENTS

        # Full markets list for comprehensive odds data
        self.markets = [
            # "player_points",
            # "player_rebounds",
            # "player_assists",
            # "player_threes",
            # "player_blocks",
            # "player_steals",
            # "player_blocks_steals",
            # "player_turnovers",
            # "player_points_rebounds_assists",
            # "player_points_rebounds",
            # "player_points_assists",
            # "player_rebounds_assists",
            "player_field_goals",
            "player_frees_made",
            "player_frees_attempts",
            # "player_double_double",
            # "player_triple_double",
        ]

        # API configuration
        self.sport = "basketball_nba"
        self.regions = "us"
        self.odds_format = "decimal"

        # Cost tracking
        self.total_credits_used = 0
        self.events_processed = 0

    def get_estimated_cost_per_event(self) -> int:
        """Get estimated cost per event based on current markets."""
        return len(self.markets) * 10  # 10 credits per market for historical event odds

    def log_cost_estimate(self, event_count: int):
        """Log cost estimate for processing events."""
        cost_per_event = self.get_estimated_cost_per_event()
        total_estimated_cost = cost_per_event * event_count
        print(
            f"💰 Cost estimate: {cost_per_event} credits/event × {event_count} events = {total_estimated_cost} credits"
        )
        print(f"🏷️  Markets: {len(self.markets)} markets")

    def get_event_ids_directly(
        self, start_date: date = None, end_date: date = None
    ) -> Dict[str, str]:
        """
        Get event IDs directly using SmartbettingLib - SUPER SIMPLE!

        Uses the integrated method in SmartbettingLib for seamless event extraction.
        Perfect for Cloud Run - no external dependencies, just one method call.

        Args:
            start_date: Start date for event extraction (optional)
            end_date: End date for event extraction (optional)

        Returns:
            Dictionary mapping event ID to commence time
        """
        try:
            # Use SmartbettingLib's integrated method - ONE simple call!
            event_data = self.smartbetting.extract_event_ids_from_historical_data(
                bucket_name=str(self.bucket),
                catalog=str(self.catalog),
                table=str(self.input_table),
                season=str(self.season),
                start_date=start_date,
                end_date=end_date,
            )

            return event_data

        except Exception as e:
            print(f"❌ Error getting event data: {e}")
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

            # Fetch historical odds from ALL bookmakers
            odds_data = self.theoddsapi.get_historical_event_odds(
                sport=self.sport,
                event_id=event_id,
                date=historical_date,
                regions=self.regions,
                markets=markets_str,
                odds_format=self.odds_format,
                bookmakers="fanduel",  # -> Gets ALL available bookmakers
            )

            return odds_data

        except Exception as e:
            print(f"Error fetching odds for event {event_id}: {e}")
            return None

    def save_event_odds_to_storage(
        self,
        event_id: str,
        odds_data: Dict[str, Any],
        historical_date: str,
        bookmakers: str,
    ) -> str:
        """
        Save historical odds data for a specific event to GCS.

        Args:
            event_id: The event ID
            odds_data: The odds data to save
            historical_date: The historical date used for the query
            bookmakers: The bookmakers used for the query
        Returns:
            The GCS blob name where the file was saved
        """
        # Create filename: raw_odds_historical_event_odds_{bookmaker}_{event_id}_{date}.json
        date_str = historical_date.split("T")[0]  # Extract date part
        filename = (
            f"raw_{self.catalog}_{self.table}_{bookmakers}_{event_id}_{date_str}.json"
        )
        gcs_path = f"{self.catalog}/{self.table}/{self.season}/{bookmakers}/{filename}"

        try:
            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = self.smartbetting.convert_to_ndjson(odds_data)

            # Upload to GCS
            self.smartbetting.upload_json_to_gcs(ndjson_data, self.bucket, gcs_path)

            print(f"✅ Saved odds for event {event_id} to {gcs_path}")
            return gcs_path

        except Exception as e:
            print(f"❌ Error saving odds for event {event_id}: {e}")
            return None

    def extract_and_save_historical_odds(
        self,
        start_date: date = None,
        end_date: date = None,
        max_events: int = None,
        delay_seconds: int = 2,
    ) -> Dict[str, str]:
        """
        SIMPLE: Extract and save historical odds for events using full markets.

        Perfect for Cloud Run - no complex file I/O, just direct method calls.

        Args:
            start_date: Start date for event extraction (optional)
            end_date: End date for event extraction (optional)
            max_events: Maximum number of events to process (for testing)
            delay_seconds: Delay between API calls (default: 2s)

        Returns:
            Dictionary mapping event ID to saved file path
        """
        print("🚀 SIMPLE Historical Event Odds Extraction")
        print(f"📅 Date range: {start_date} to {end_date}")
        print("=" * 60)

        # Get event IDs directly (no file I/O!)
        event_data = self.get_event_ids_directly(start_date, end_date)
        if not event_data:
            print("❌ No event data found")
            return {}

        # Apply testing limit if specified
        if max_events and len(event_data) > max_events:
            event_items = list(event_data.items())[:max_events]
            event_data = dict(event_items)
            print(f"🧪 Testing mode: Limited to {max_events} events")

        # Log cost estimate and start processing
        self.log_cost_estimate(len(event_data))
        print(f"\n⏱️  Processing {len(event_data)} events...")
        print("-" * 60)

        saved_files = {}
        processed = 0
        failed = 0
        start_time = time.time()

        for event_id, commence_time in event_data.items():
            try:
                processed += 1
                event_cost = self.get_estimated_cost_per_event()

                print(
                    f"\n[{processed}/{len(event_data)}] Processing event: {event_id[:20]}..."
                )
                print(f"💰 Cost: {event_cost} credits | ⏰ Commence: {commence_time}")

                # Calculate historical date
                historical_date = self.calculate_historical_date(commence_time)

                # Fetch historical odds
                odds_data = self.fetch_historical_odds_for_event(
                    event_id, historical_date
                )

                if odds_data:
                    # Count bookmakers found
                    bookmakers_found = set()
                    if isinstance(odds_data, list):
                        for market_data in odds_data:
                            if "bookmakers" in market_data:
                                for bookmaker in market_data["bookmakers"]:
                                    bookmakers_found.add(
                                        bookmaker.get("key", "unknown")
                                    )

                    # Save to storage
                    bookmaker_str = "fanduel"  # Since we're filtering to only fanduel
                    saved_path = self.save_event_odds_to_storage(
                        event_id, odds_data, historical_date, bookmaker_str
                    )
                    if saved_path:
                        saved_files[event_id] = saved_path
                        self.total_credits_used += event_cost
                        self.events_processed += 1
                        print(
                            f"✅ Success | Bookmakers: {len(bookmakers_found)} | Credits used: {self.total_credits_used}"
                        )
                        if bookmakers_found:
                            print(
                                f"📊 Bookmakers found: {', '.join(sorted(bookmakers_found))}"
                            )
                else:
                    failed += 1
                    print("⚠️  No odds data available")

                # Rate limiting delay
                if processed < len(event_data):
                    print(f"⏳ Waiting {delay_seconds}s...")
                    time.sleep(delay_seconds)

            except Exception as e:
                failed += 1
                print(f"❌ Error: {str(e)}")
                continue

        # Calculate execution time
        execution_time = time.time() - start_time

        # Print comprehensive summary
        print("\n" + "=" * 80)
        print("📊 HISTORICAL EVENT ODDS EXTRACTION SUMMARY:")
        print("=" * 80)
        print(f"📅 Date range: {start_date} to {end_date}")
        print(f"🎯 Events processed: {processed}")
        print(f"✅ Successfully saved: {len(saved_files)}")
        print(f"❌ Failed: {failed}")
        print(f"💰 Total credits used: {self.total_credits_used}")
        print(f"⏱️  Execution time: {execution_time:.1f} seconds")
        print(f"🏷️  Markets: {len(self.markets)} full markets")
        print(
            f"📁 Success rate: {(len(saved_files) / processed) * 100:.1f}%"
            if processed > 0
            else "N/A"
        )
        print("=" * 80)

        return saved_files


def main():
    """
    SIMPLE main function - perfect for Cloud Run pipelines.

    Gets event IDs directly from SmartbettingLib and processes them.
    No complex file operations, just straightforward method calls.
    """
    extractor = HistoricalEventOddsExtractor()

    # Simple configuration
    start_date = date(2025, 4, 7)  # Can be None for all events
    end_date = date(2025, 4, 10)  # Can be None for all events
    max_events = None  # Set to None for production

    print("🎯 SIMPLE HISTORICAL EVENT ODDS EXTRACTION")
    print("=" * 50)
    print(f"📅 Date range: {start_date} to {end_date}")
    print(f"🎛️  Markets: {len(extractor.markets)} full markets")
    if max_events:
        print(f"🧪 Testing: {max_events} events max")
    print("=" * 50)

    # Extract odds - ONE simple method call
    saved_files = extractor.extract_and_save_historical_odds(
        start_date=start_date,
        end_date=end_date,
        max_events=max_events,
    )

    # Final summary
    if saved_files:
        print(f"\n🎉 Successfully saved {len(saved_files)} event odds files")
        print(f"💰 Total estimated credits used: {extractor.total_credits_used}")

    else:
        print("❌ No event odds files were saved")


if __name__ == "__main__":
    main()
