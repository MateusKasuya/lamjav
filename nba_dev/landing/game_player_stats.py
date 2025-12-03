"""
NBA Game Player Stats data pipeline script.

This script fetches NBA player stats data from the Balldontlie API for both specific dates
and entire seasons, then uploads it to Google Cloud Storage in the landing layer.
"""

import sys
import os
import time
from typing import NoReturn
from datetime import date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA game player stats data pipeline.

    This function:
    1. Fetches player stats data from Balldontlie API for each day starting from 22/10/2024
    2. Fetches player stats data for the entire 2024 season
    3. Converts the data to the required format
    4. Uploads the data to Google Cloud Storage in the landing layer
    5. Includes longer delays between each date extraction to avoid rate limiting

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.NBA
    table = Table.GAME_PLAYER_STATS
    season = Season.SEASON_2025

    # Set start date for daily extraction
    # Use yesterday's date since today's games haven't finished yet
    start_date = date(2025,11,28)  # 22/10/2024
    end_date =date(2025,12,1)  # Today's date

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting NBA game player stats data pipeline")
        print(f"Season: {season}")
        print(f"Daily extraction from {start_date} to {end_date}")
        print("=" * 80)

        # PART 1: Extract player stats by date (daily)
        print("\n" + "=" * 60)
        print("EXTRACTING PLAYER STATS BY DATE (DAILY)")
        print("=" * 60)

        current_date = start_date
        total_stats_by_date = 0

        while current_date <= end_date:
            print(f"\nProcessing player stats for date: {current_date}")

            # Fetch player stats data from API for current date
            response = balldontlie.get_stats(current_date)

            if response is None:
                print(f"No player stats data received for {current_date}")
                current_date += timedelta(days=1)
                # Add small delay even when no data
                time.sleep(2)
                continue

            if len(response) == 0:
                print(f"No player stats found for {current_date}")
                current_date += timedelta(days=1)
                # Add small delay even when no data
                time.sleep(2)
                continue

            # Convert API response to dictionary format
            data = smartbetting.convert_object_to_dict(response)

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

            # Upload NDJSON data to Google Cloud Storage (by date)
            gcs_blob_name = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{current_date.strftime('%Y-%m-%d')}.json"
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully processed and uploaded {len(data)} player stats for {current_date}"
            )
            total_stats_by_date += len(data)

            # Add moderate delay between date extractions
            print("Waiting 5 seconds before processing next date...")
            time.sleep(5)

            # Move to next date
            current_date += timedelta(days=1)

        print(
            f"\n📊 Daily extraction completed! Total player stats by date: {total_stats_by_date}"
        )

        # Print overall summary
        print("\n" + "=" * 80)
        print("OVERALL PLAYER STATS EXTRACTION SUMMARY:")
        print(f"📅 Stats by date: {total_stats_by_date}")
        print(f"🎯 Season: {season}")
        print(f"📅 Date range: {start_date} to {end_date}")

    except Exception as e:
        print(f"Error in game player stats data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
