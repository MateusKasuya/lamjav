"""
The Odds API NBA Historical Events data pipeline script.

This script fetches NBA historical events data from The Odds API for each day
starting from 22/10/2024 and uploads it to Google Cloud Storage in the
landing layer of the data lake.

⚠️  WARNING: This endpoint costs 1 credit per request and requires a paid plan!
"""

import sys
import os
from typing import NoReturn
from datetime import date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the The Odds API NBA historical events data pipeline.

    This function:
    1. Fetches historical events data from The Odds API for each day starting from 22/10/2024
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
    table = Table.HISTORICAL_EVENTS
    season = Season.SEASON_2024

    # Set start date (same as games.py)
    start_date = date(2025, 4, 9)
    end_date = date(2025, 4, 10)  # Today's date

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        current_date = start_date
        total_events_processed = 0
        total_requests_made = 0

        print(
            f"Starting historical events data pipeline from {start_date} to {end_date}"
        )
        print("⚠️  WARNING: This will cost 1 credit per day!")

        while current_date <= end_date:
            print(f"\nProcessing historical events for date: {current_date}")

            # Convert date to ISO8601 format for the API
            # Use noon time to ensure we get a good snapshot
            api_date = current_date.strftime("%Y-%m-%dT12:00:00Z")

            # Fetch historical events data from API for current date
            response = theoddsapi.get_historical_events(
                sport="basketball_nba", date=api_date
            )

            if response is None:
                print(f"No historical events data received for {current_date}")
                current_date += timedelta(days=1)
                continue

            # Extract data from response
            events_data = response.get("data", [])

            if len(events_data) == 0:
                print(f"No historical events found for {current_date}")
                current_date += timedelta(days=1)
                continue

            # The Odds API returns native Python dictionaries, not Pydantic objects
            # So we can use the data directly without conversion
            data = events_data

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

            # Upload NDJSON data to Google Cloud Storage
            gcs_blob_name = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{current_date.strftime('%Y-%m-%d')}.json"
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            # Extract metadata for logging
            timestamp = response.get("timestamp", "Unknown")

            print(
                f"Successfully processed and uploaded {len(data)} historical events for {current_date}"
            )
            print(f"Snapshot timestamp: {timestamp}")
            print(f"File saved as: {gcs_blob_name}")

            total_events_processed += len(data)
            total_requests_made += 1

            # Move to next date
            current_date += timedelta(days=1)

        print("\nPipeline completed!")
        print(f"Total historical events processed: {total_events_processed}")
        print(f"Total API requests made: {total_requests_made}")
        print(f"Total credits used: {total_requests_made}")

    except Exception as e:
        print(f"Error in historical events data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
