"""
The Odds API NBA Current Events data pipeline script.

This script fetches ALL upcoming NBA events from The Odds API and uploads it
to Google Cloud Storage in the landing layer of the data lake.

This creates a snapshot of all open/upcoming events, regardless of when they occur.
The file is overwritten on each execution with the latest snapshot.
"""

import sys
import os
from typing import NoReturn
from datetime import datetime

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the The Odds API NBA current events data pipeline.

    This function:
    1. Fetches ALL upcoming/open events data from The Odds API (no date filters)
    2. Converts the data to NDJSON format
    3. Uploads the data to Google Cloud Storage with a standard filename

    Note: This endpoint returns all future events regardless of when they occur.
    The file is overwritten on each run to provide the latest snapshot.

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    table = Table.EVENTS
    season = Season.SEASON_2025

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting current events pipeline - fetching ALL upcoming events")

        # Fetch ALL upcoming events (no date filters)
        events = theoddsapi.get_events(
            sport="basketball_nba",
            date_format="iso",
        )

        if events is None:
            print("No events data received from API")
            raise Exception("Failed to fetch events data")

        if len(events) == 0:
            print("No upcoming events found")
            return

        print(f"Successfully fetched {len(events)} upcoming events")

        # Convert to NDJSON
        ndjson_data = smartbetting.convert_to_ndjson(events)

        # Upload NDJSON data to Google Cloud Storage with standard filename
        gcs_blob_name = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{datetime.now().strftime('%Y-%m-%d')}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

        print("Successfully uploaded events snapshot to GCS")
        print(f"File saved as: {gcs_blob_name}")
        print(f"Total events in snapshot: {len(events)}")

    except Exception as e:
        print(f"Error in current events data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
