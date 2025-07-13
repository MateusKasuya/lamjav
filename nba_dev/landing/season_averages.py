"""
NBA Season Averages data pipeline script.

This script fetches NBA season averages data from the Balldontlie API for
general/base category and uploads to Google Cloud Storage.
"""

import sys
import os
from typing import NoReturn
from datetime import date

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


def main() -> NoReturn:
    """
    Main function to fetch NBA season averages data and upload to GCS.

    This function:
    1. Fetches general/base season averages for 2024 regular season
    2. Uploads the data to Google Cloud Storage

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
    table = Table.SEASON_AVERAGES

    # Season averages parameters
    season = 2024
    season_type = "regular"
    category = "general"
    type_param = "base"

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        # Fetch season averages data from API
        response = balldontlie.get_season_averages(
            category, season_type, type_param, season
        )

        if response is None or len(response) == 0:
            raise Exception(
                "Failed to fetch season averages data from API or no data received"
            )

        # Convert API response to dictionary format (if needed)
        if response and hasattr(response[0], "model_dump"):
            data = smartbetting.convert_object_to_dict(response)
        else:
            data = response  # Already in dictionary format

        # Convert data to JSON format
        json_data = smartbetting.convert_to_json(data)

        # Upload JSON data to Google Cloud Storage
        extraction_date = date.today().strftime("%Y-%m-%d")
        gcs_blob_name = f"{catalog}/{schema}/{table}/season_averages_{category}_{type_param}_{season}_{extraction_date}.json"
        smartbetting.upload_json_to_gcs(json_data, bucket, gcs_blob_name)

        print(
            f"Successfully processed and uploaded {len(data)} season averages to Google Cloud Storage"
        )

    except Exception as e:
        print(f"Error in season averages pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
