"""
NBA Leaders data pipeline script.

This script fetches NBA leaders data from the Balldontlie API for various stat types
and seasons, then uploads the data to Google Cloud Storage in the landing layer.
"""

import sys
import os
from typing import NoReturn

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA leaders data pipeline.

    This function:
    1. Fetches NBA leaders data for multiple stat types and seasons
    2. Converts the data to the required format
    3. Uploads the data to Google Cloud Storage in the landing layer
    4. Includes proper error handling and logging

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.NBA
    table = Table.LEADERS

    # Define stat types and seasons to fetch
    stat_types = ["pts", "reb", "ast", "stl", "blk", "min", "tov", "oreb", "dreb"]
    seasons = [Season.SEASON_2024]  # Current season

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        total_leaders_processed = 0
        failed_requests = []

        print(
            f"Starting NBA leaders data pipeline for {len(stat_types)} stat types and {len(seasons)} seasons"
        )

        for season in seasons:
            for stat_type in stat_types:
                try:
                    print(
                        f"\nProcessing leaders for stat_type: {stat_type}, season: {season}"
                    )

                    # Fetch leaders data from API
                    response = balldontlie.get_leaders(
                        stat_type=stat_type, season=season
                    )

                    if response is None or len(response) == 0:
                        print(
                            f"No leaders data received for {stat_type} in season {season}"
                        )
                        failed_requests.append(
                            (stat_type, season, "No response from API")
                        )
                        continue

                    # Convert API response to dictionary format
                    data = smartbetting.convert_object_to_dict(response)

                    # Convert data to NDJSON format for BigQuery compatibility
                    ndjson_data = smartbetting.convert_to_ndjson(data)

                    # Upload NDJSON data to Google Cloud Storage
                    gcs_blob_name = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{stat_type}_{season}.json"
                    smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

                    print(
                        f"Successfully processed and uploaded {len(data)} leaders for {stat_type} in season {season}"
                    )
                    total_leaders_processed += len(data)

                    # Add small delay between requests to respect API rate limits
                    import time

                    time.sleep(2)

                except Exception as e:
                    error_msg = (
                        f"Error processing {stat_type} for season {season}: {str(e)}"
                    )
                    print(error_msg)
                    failed_requests.append((stat_type, season, str(e)))

        print(
            f"\nPipeline completed! Total leaders processed: {total_leaders_processed}"
        )

        if failed_requests:
            print(f"\nFailed requests ({len(failed_requests)}):")
            for stat_type, season, error in failed_requests:
                print(f"  {stat_type} - Season {season}: {error}")

    except Exception as e:
        print(f"Error in NBA leaders data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
