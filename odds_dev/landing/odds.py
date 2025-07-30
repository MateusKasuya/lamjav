"""
The Odds API NBA Odds data pipeline script.

This script fetches NBA odds data from The Odds API and uploads
it to S3 in the bronze layer of the data lake.
"""

import sys
import os
from typing import NoReturn
from datetime import date

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


def main() -> NoReturn:
    """
    Main function to execute the The Odds API NBA odds data pipeline.

    This function:
    1. Fetches NBA odds data from The Odds API
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
    catalog = Catalog.ODDS
    schema = Schema.LANDING
    table = Table.ODDS

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        # Fetch NBA odds data from API
        # Using h2h,spreads,totals for comprehensive data
        # This costs 3 credits per request (1 per market)
        response = theoddsapi.get_odds(
            sport="basketball_nba",
            regions="us",
            markets="h2h,spreads,totals",
            odds_format="decimal",  # Using decimal format for consistency
        )

        if response is None:
            raise Exception("Failed to fetch NBA odds data from API")

        # The Odds API returns native Python dictionaries, not Pydantic objects
        # So we can use the data directly without conversion
        data = response

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to GCS
        gcs_key = f"{catalog}/{schema}/{table}/{table}_{date.today().strftime('%Y-%m-%d')}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_key)

        print(f"Successfully processed and uploaded {len(data)} NBA odds events to GCS")

    except Exception as e:
        print(f"Error in NBA odds data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
