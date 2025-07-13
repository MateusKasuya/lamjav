"""
NBA Games data pipeline script.

This script fetches NBA games data from the Balldontlie API for each day
starting from 22/10/2024 and uploads it to Google Cloud Storage in the
landing layer of the data lake.
"""

import sys
import os
from typing import NoReturn
from datetime import date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


def main() -> NoReturn:
    """
    Main function to execute the NBA games data pipeline.

    This function:
    1. Fetches games data from Balldontlie API for each day starting from 22/10/2024
    2. Converts the data to the required format
    3. Uploads the data to Google Cloud Storage in the landing layer

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.LAMJAV_STORAGE
    catalog = Catalog.NBA
    schema = Schema.LANDING
    table = Table.GAMES

    # Set start date
    start_date = date(2024, 10, 22)
    end_date = date.today()  # Today's date

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        current_date = start_date
        total_games_processed = 0

        print(f"Starting games data pipeline from {start_date} to {end_date}")

        while current_date <= end_date:
            print(f"\nProcessing games for date: {current_date}")

            # Fetch games data from API for current date
            response = balldontlie.get_games(current_date)

            if response is None:
                print(f"No games data received for {current_date}")
                current_date += timedelta(days=1)
                continue

            if len(response) == 0:
                print(f"No games found for {current_date}")
                current_date += timedelta(days=1)
                continue

            # Convert API response to dictionary format
            data = smartbetting.convert_object_to_dict(response)

            # Convert data to JSON format
            json_data = smartbetting.convert_to_json(data)

            # Upload JSON data to Google Cloud Storage
            gcs_blob_name = f"{catalog}/{schema}/{table}/{table}_{current_date.strftime('%Y-%m-%d')}.json"
            smartbetting.upload_json_to_gcs(json_data, bucket, gcs_blob_name)

            print(
                f"Successfully processed and uploaded {len(data)} games for {current_date}"
            )
            total_games_processed += len(data)

            # Move to next date
            current_date += timedelta(days=1)

        print(f"\nPipeline completed! Total games processed: {total_games_processed}")

    except Exception as e:
        print(f"Error in games data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
