"""
NBA Active Players data pipeline script.

This script fetches active NBA players data from the Balldontlie API and uploads
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
    Main function to execute the NBA active players data pipeline.

    This function:
    1. Fetches active players data from Balldontlie API
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
    table = Table.ACTIVE_PLAYERS

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting active players data pipeline")

        # Fetch active players data from API
        response = balldontlie.get_active_players()

        if response is None:
            raise Exception("Failed to fetch active players data from API")

        if len(response) == 0:
            raise Exception("No active players data received from API")

        # Convert API response to dictionary format
        data = smartbetting.convert_object_to_dict(response)

        # Convert data to JSON format
        json_data = smartbetting.convert_to_json(data)

        # Upload JSON data to Google Cloud Storage
        gcs_blob_name = f"{catalog}/{schema}/{table}/{table}_{date.today().strftime('%Y-%m-%d')}.json"
        smartbetting.upload_json_to_gcs(json_data, bucket, gcs_blob_name)

        print(
            f"Successfully processed and uploaded {len(data)} active players to Google Cloud Storage"
        )

    except Exception as e:
        print(f"Error in active players data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
