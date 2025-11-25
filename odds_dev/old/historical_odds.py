"""
The Odds API NBA Historical Odds data pipeline script.

This script fetches NBA historical odds data from The Odds API and uploads
it to S3 in the bronze layer of the data lake.

⚠️  WARNING: This endpoint costs 10 credits per market per region and requires a paid plan!
"""

import sys
import os
from typing import NoReturn
from datetime import datetime, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


def main() -> NoReturn:
    """
    Main function to execute the The Odds API NBA historical odds data pipeline.

    This function:
    1. Fetches NBA historical odds data from The Odds API
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
    table = Table.HISTORICAL_ODDS

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        # Calculate a recent date for historical data
        # Use yesterday at noon for testing
        yesterday = datetime.now() - timedelta(days=1)
        historical_date = yesterday.replace(
            hour=12, minute=0, second=0, microsecond=0
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        print("⚠️  WARNING: This will cost 30 credits (3 markets x 10 credits each)!")
        print(f"📅 Fetching historical data for: {historical_date}")

        # Fetch NBA historical odds data from API
        # Using h2h,spreads,totals for comprehensive data
        # This costs 30 credits per request (3 markets x 10 credits each)
        response = theoddsapi.get_historical_odds(
            sport="basketball_nba",
            date=historical_date,
            regions="us",
            markets="h2h,spreads,totals",
            odds_format="decimal",
        )

        if response is None:
            raise Exception("Failed to fetch NBA historical odds data from API")

        # The Odds API returns native Python dictionaries, not Pydantic objects
        # So we can use the data directly without conversion
        data = response

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to GCS
        # Include the historical date in the filename
        date_str = historical_date.split("T")[0]  # Extract just the date part
        gcs_key = f"{catalog}/{schema}/{table}/{table}_{date_str}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_key)

        # Extract metadata for logging
        timestamp = data.get("timestamp", "Unknown")
        data_points = len(data.get("data", []))

        print(
            f"Successfully processed and uploaded {data_points} NBA historical odds events to GCS"
        )
        print(f"Historical snapshot timestamp: {timestamp}")
        print(f"File saved as: {gcs_key}")

    except Exception as e:
        print(f"Error in NBA historical odds data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
