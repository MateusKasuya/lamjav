"""
NBA Team Standings data pipeline script.

This script fetches NBA team standings data from the Balldontlie API for
the 2024 season and uploads to Google Cloud Storage.
"""

import sys
import os
from typing import NoReturn
from datetime import date

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog


def main() -> NoReturn:
    """
    Main function to fetch NBA team standings data and upload to GCS.

    This function:
    1. Fetches team standings for 2024 season
    2. Uploads the data to Google Cloud Storage

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.NBA
    table = "team_standings"  # Using string since it's not in the enum

    # Season parameter
    season = 2024

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        # Fetch team standings data from API
        response = balldontlie.get_team_standings(season)

        if response is None or len(response) == 0:
            raise Exception(
                "Failed to fetch team standings data from API or no data received"
            )

        # Convert API response to dictionary format
        data = smartbetting.convert_object_to_dict(response)

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to Google Cloud Storage
        extraction_date = date.today().strftime("%Y-%m-%d")
        gcs_blob_name = f"{catalog}/{table}/raw_{catalog}_{table}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

        print(
            f"Successfully processed and uploaded {len(data)} team standings to Google Cloud Storage"
        )

    except Exception as e:
        print(f"Error in team standings pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
