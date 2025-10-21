"""
Extract current Event Odds from The Odds API.

This script reads event IDs and commence times from the current events data
and fetches current odds for each event from The Odds API, then saves them
to Google Cloud Storage in odds/landing/event_odds.
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
    start_date = None
    end_date = None
    max_events = None
    delay_seconds = 2
    
    # Markets configuration (start lean; can be expanded)
    markets = [
        # "h2h",
        # "totals", 
        # "spreads",
    ]
    markets_str = ",".join(markets) if markets else None
    
    sport = "basketball_nba"
    regions = "us"
    odds_format = "decimal"
    bookmakers = "fanduel"

    print("🎯 CURRENT EVENT ODDS EXTRACTION")
    print("=" * 50)
    print(f"📅 Date range: {start_date} to {end_date}")
    print("=" * 50)

    # Extract event IDs from current events data
    event_data = smartbetting.extract_event_ids_from_events_data(
        bucket_name=str(bucket),
        catalog=str(catalog),
        table=str(input_table),
        season=str(season),
        start_date=start_date,
        end_date=end_date,
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


