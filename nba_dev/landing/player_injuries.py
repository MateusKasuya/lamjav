"""
NBA Player Injuries data pipeline script.

This script fetches NBA player injuries data from the Balldontlie API and uploads
it to Google Cloud Storage in the landing layer of the data lake.
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
    Main function to execute the NBA player injuries data pipeline.

    This function:
    1. Fetches player injuries data from Balldontlie API
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
    table = Table.PLAYER_INJURIES

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting player injuries data pipeline")

        # Fetch player injuries data from API
        response = balldontlie.get_injuries()

        if response is None:
            raise Exception("Failed to fetch player injuries data from API")

        if len(response) == 0:
            print(
                "No player injuries data found - this might be normal if no players are currently injured"
            )
            # Create empty file to indicate the pipeline ran
            ndjson_data = smartbetting.convert_to_ndjson([])
        else:
            # Convert API response to dictionary format
            data = smartbetting.convert_object_to_dict(response)
            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to Google Cloud Storage
        gcs_blob_name = f"{catalog}/{schema}/{table}/{table}_{date.today().strftime('%Y-%m-%d')}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

        if len(response) == 0:
            print(
                "Successfully uploaded empty player injuries file to Google Cloud Storage"
            )
        else:
            print(
                f"Successfully processed and uploaded {len(response)} player injuries to Google Cloud Storage"
            )

    except Exception as e:
        print(f"Error in player injuries data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
