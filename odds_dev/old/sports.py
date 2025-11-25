"""
The Odds API Sports data pipeline script.

This script fetches sports data from The Odds API and uploads
it to S3 in the bronze layer of the data lake.
"""

import sys
import os
from typing import NoReturn

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the The Odds API sports data pipeline.

    This function:
    1. Fetches sports data from The Odds API
    2. Converts the data to the required format
    3. Uploads the data to Google Cloud Storage in the landing layer

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    table = Table.SPORTS
    season = Season.SEASON_2024

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        # Fetch sports data from API (both in-season and out-of-season)
        response = theoddsapi.get_sports(all_sports=True)

        if response is None:
            raise Exception("Failed to fetch sports data from API")

        # The Odds API returns native Python dictionaries, not Pydantic objects
        # So we can use the data directly without conversion
        data = response

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to GCS
        gcs_key = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{season}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_key)

        print(f"Successfully processed and uploaded {len(data)} sports to GCS")

    except Exception as e:
        print(f"Error in sports data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
