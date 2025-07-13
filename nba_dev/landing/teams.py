"""
NBA Teams data pipeline script.

This script fetches NBA teams data from the Balldontlie API and uploads
it to S3 in the bronze layer of the data lake.
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
    Main function to execute the NBA teams data pipeline.

    This function:
    1. Fetches teams data from Balldontlie API
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
    table = Table.TEAMS

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        # Fetch teams data from API
        response = balldontlie.get_teams()

        if response is None:
            raise Exception("Failed to fetch teams data from API")

        # Convert API response to dictionary format
        data = smartbetting.convert_object_to_dict(response)

        # Convert data to JSON format
        json_data = smartbetting.convert_to_json(data)

        # Upload JSON data to GCS
        gcs_key = f"{catalog}/{schema}/{table}/{table}_{date.today().strftime('%Y-%m-%d')}.json"
        smartbetting.upload_json_to_gcs(json_data, bucket, gcs_key)

        print(f"Successfully processed and uploaded {len(data)} teams to GCS")

    except Exception as e:
        print(f"Error in teams data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
