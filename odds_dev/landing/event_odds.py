"""
Extract current Event Odds from The Odds API.

This script reads ALL event IDs from the current events snapshot
and fetches current odds for each event from The Odds API, then saves them
to Google Cloud Storage.

This processes all open/upcoming events regardless of their commence time.
Each event's odds are saved in a separate file for granular data management.
"""

import sys
import os
from datetime import date

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Season, Table


def main():
    """
    Main function to extract and save current event odds.

    This function:
    1. Reads ALL event IDs from the events snapshot (no date filtering)
    2. Fetches current odds for each event from The Odds API
    3. Saves odds for each event in separate files in GCS

    Note: This processes all open events. Use max_events for testing/limiting.
    """
    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    # Configuration
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    season = Season.SEASON_2025
    table = Table.EVENT_ODDS
    input_table = Table.EVENTS

    # Parameters
    max_events = None  # Set to a number to limit for testing
    delay_seconds = 2  # Delay between API calls to respect rate limits

    # Markets configuration (start lean; can be expanded)
    markets = [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_threes",
        "player_blocks",
        "player_steals",
        "player_blocks_steals",
        "player_turnovers",
        "player_points_rebounds_assists",
        "player_points_rebounds",
        "player_points_assists",
        "player_rebounds_assists",
        # "player_field_goals",
        # "player_frees_made",
        # "player_frees_attempts",
        "player_double_double",
        "player_triple_double",
    ]
    markets_str = ",".join(markets) if markets else None

    sport = "basketball_nba"
    regions = "us"
    odds_format = "decimal"
    bookmakers = "draftkings"

    # Date filter - only today's events
    today = date.today()

    print("🎯 CURRENT EVENT ODDS EXTRACTION")
    print("=" * 60)
    print(f"📅 Processing events for TODAY: {today}")
    if max_events:
        print(f"🧪 Testing mode: Limited to {max_events} events")
    print("=" * 60)

    # Extract event IDs directly from EVENTS (optimal for Cloud Functions)
    # Filter by today's date to get only current games
    print(f"\n📊 Extracting event IDs for {today}...")
    event_data = smartbetting.extract_event_ids_from_events_data(
        bucket_name=str(bucket),
        catalog=str(catalog),
        table=str(input_table),
        season=str(season),
        start_date=today,
        end_date=today,
    )

    if not event_data:
        print("❌ No event data found")
        return

    # Extract and save odds using the centralized function
    saved_files = theoddsapi.extract_and_save_event_odds(
        event_data=event_data,
        smartbetting_lib=smartbetting,
        bucket_name=str(bucket),
        catalog=str(catalog),
        table=str(table),
        season=str(season),
        sport=sport,
        regions=regions,
        markets=markets_str,
        odds_format=odds_format,
        bookmakers=bookmakers,
        max_events=max_events,
        delay_seconds=delay_seconds,
    )

    if saved_files:
        print(f"\n🎉 Successfully saved {len(saved_files)} event odds files")
    else:
        print("❌ No event odds files were saved")


if __name__ == "__main__":
    main()
