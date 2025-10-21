"""
NBA Games data pipeline script with datetime preservation.

This script fetches NBA games data from the Balldontlie API for both specific dates
and entire seasons, then uploads it to Google Cloud Storage in the landing layer.
"""

import sys
import os
from typing import NoReturn
from datetime import date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA games data pipeline.

    This function fetches games data by date using a datetime-preserving
    method and uploads it to Google Cloud Storage in the landing layer.

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.NBA
    table = Table.GAMES
    season = Season.SEASON_2025

    # Set start date for daily extraction
    start_date = date.today()  # 22/10/2024
    end_date = date(2025,10,30)  # Today's date

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting NBA games data pipeline")
        print(f"Season: {season}")
        print(f"Daily extraction from {start_date} to {end_date}")
        print("=" * 80)

        # PART 1: Extract games by date (daily)
        print("\n" + "=" * 60)
        print("EXTRACTING GAMES BY DATE (DAILY)")
        print("=" * 60)

        current_date = start_date
        total_games_by_date = 0

        while current_date <= end_date:
            print(f"\nProcessing games for date: {current_date}")

            # Fetch games data using datetime-preserving method
            response = balldontlie.get_games_with_datetime(current_date)

            if response is None:
                print(f"No games data received for {current_date}")
                current_date += timedelta(days=1)
                continue

            if len(response) == 0:
                print(f"No games found for {current_date}")
                current_date += timedelta(days=1)
                continue

            # Verify datetime field is present
            games_with_datetime = [g for g in response if g.get("datetime")]
            games_without_datetime = [g for g in response if not g.get("datetime")]

            print(f"Found {len(response)} games for {current_date}")
            print(f"  Games with datetime: {len(games_with_datetime)}")
            print(f"  Games without datetime: {len(games_without_datetime)}")

            if games_with_datetime:
                # Show example of datetime field
                example_game = games_with_datetime[0]
                print(f"  Example datetime: {example_game.get('datetime')}")

            # Data is already in dictionary format from get_games_with_datetime()
            # No need to convert since we're using direct requests
            data = response

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

            # Upload NDJSON data to Google Cloud Storage (by date)
            gcs_blob_name = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{current_date.strftime('%Y-%m-%d')}.json"
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully processed and uploaded {len(data)} games for {current_date}"
            )
            total_games_by_date += len(data)

            # Move to next date
            current_date += timedelta(days=1)

        print(
            f"\n📊 Daily extraction completed! Total games by date: {total_games_by_date}"
        )

        # Print overall summary
        print("\n" + "=" * 80)
        print("OVERALL GAMES EXTRACTION SUMMARY:")
        print(f"📅 Games by date: {total_games_by_date}")
        print(f"🎯 Season: {season}")
        print(f"📅 Date range: {start_date} to {end_date}")

    except Exception as e:
        print(f"Error in games data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
