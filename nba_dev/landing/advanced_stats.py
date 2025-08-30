"""
NBA Advanced Stats data pipeline script.

This script fetches NBA advanced stats data from the Balldontlie API for both specific dates
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
    Main function to execute the NBA advanced stats data pipeline.

    This function:
    1. Fetches advanced stats data from Balldontlie API for each day starting from 22/10/2024
    2. Fetches advanced stats data for the entire 2024 season
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
    table = Table.ADVANCED_STATS
    season = Season.SEASON_2024

    # Set start date for daily extraction
    start_date = date(2025, 1, 1)
    end_date = date.today()  # Today's date

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting NBA advanced stats data pipeline")
        print(f"Season: {season}")
        print(f"Daily extraction from {start_date} to {end_date}")
        print("=" * 80)

        # PART 1: Extract advanced stats by date (daily)
        print("\n" + "=" * 60)
        print("EXTRACTING ADVANCED STATS BY DATE (DAILY)")
        print("=" * 60)

        current_date = start_date
        total_stats_by_date = 0

        while current_date <= end_date:
            print(f"\nProcessing advanced stats for date: {current_date}")

            # Fetch advanced stats data from API for current date
            response = balldontlie.get_advanced_stats(dates=[current_date])

            if response is None:
                print(f"No advanced stats data received for {current_date}")
                current_date += timedelta(days=1)
                # Add small delay even when no data
                time.sleep(2)
                continue

            if len(response) == 0:
                print(f"No advanced stats found for {current_date}")
                current_date += timedelta(days=1)
                # Add small delay even when no data
                time.sleep(2)
                continue

            # Convert API response to dictionary format
            data = smartbetting.convert_object_to_dict(response)

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

            # Upload NDJSON data to Google Cloud Storage (by date)
            gcs_blob_name = f"{catalog}/{table}/raw_{catalog}_{table}_{current_date.strftime('%Y-%m-%d')}.json"
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully processed and uploaded {len(data)} advanced stats for {current_date}"
            )
            total_stats_by_date += len(data)

            # Add moderate delay between date extractions
            print("Waiting 5 seconds before processing next date...")
            time.sleep(5)

            # Move to next date
            current_date += timedelta(days=1)

        print(
            f"\n📊 Daily extraction completed! Total advanced stats by date: {total_stats_by_date}"
        )

        # PART 2: Extract advanced stats by season
        print("\n" + "=" * 60)
        print("EXTRACTING ADVANCED STATS BY SEASON")
        print("=" * 60)

        print(f"Fetching all advanced stats for season {season}...")

        # Fetch advanced stats data from API for the specific season
        # Note: Using get_advanced_stats with season parameter
        season_start = date(season, 10, 1)  # October 1st of the season
        season_end = date(season + 1, 6, 30)  # June 30th of next year

        print(f"Season range: {season_start} to {season_end}")

        # For season extraction, we'll collect all daily stats
        all_season_stats = []
        current_season_date = season_start

        while current_season_date <= season_end:
            if current_season_date <= date.today():  # Only process dates up to today
                print(
                    f"Processing season advanced stats for date: {current_season_date}"
                )

                response = balldontlie.get_advanced_stats(dates=[current_season_date])

                if response and len(response) > 0:
                    # Convert API response to dictionary format
                    data = smartbetting.convert_object_to_dict(response)
                    all_season_stats.extend(data)
                    print(
                        f"  Added {len(data)} advanced stats for {current_season_date}"
                    )

                # Add delay between API calls
                time.sleep(2)

            current_season_date += timedelta(days=1)

        # Upload season data to Google Cloud Storage
        if all_season_stats:
            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(all_season_stats)

            # Upload NDJSON data to Google Cloud Storage (by season)
            gcs_blob_name = (
                f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{season}.json"
            )
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully processed and uploaded {len(all_season_stats)} advanced stats for season {season}"
            )
        else:
            # Create empty file to indicate the pipeline ran
            ndjson_data = smartbetting.convert_to_ndjson([])
            gcs_blob_name = (
                f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{season}.json"
            )
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"✅ Successfully uploaded empty advanced stats file for season {season}"
            )

        # Print overall summary
        print("\n" + "=" * 80)
        print("OVERALL ADVANCED STATS EXTRACTION SUMMARY:")
        print(f"📅 Stats by date: {total_stats_by_date}")
        print(f"🏀 Stats by season: {len(all_season_stats)}")
        print(
            f"📊 Total stats processed: {total_stats_by_date + len(all_season_stats)}"
        )
        print(f"🎯 Season: {season}")
        print(f"📅 Date range: {start_date} to {end_date}")

    except Exception as e:
        print(f"Error in advanced stats data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
