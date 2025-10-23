"""
The Odds API NBA current Events data pipeline script.

This script fetches NBA upcoming events data from The Odds API and uploads it
to Google Cloud Storage in the landing layer of the data lake.
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
    Main function to execute the The Odds API NBA current events data pipeline.

    This function:
    1. Fetches upcoming/current events data from The Odds API
    2. Converts the data to NDJSON format
    3. Uploads the data to Google Cloud Storage in the landing layer
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    table = Table.EVENTS
    season = Season.SEASON_2025

    # Define date window for file naming (allows backfilling if needed)
    start_date = date(2025,10,21) #date.today()
    end_date = date.today() + timedelta(days=1)

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        current_date = start_date
        total_events_processed = 0
        total_requests_made = 0

        print(f"Starting current events pipeline from {start_date} to {end_date}")

        while current_date <= end_date:
            print(f"\nProcessing events snapshot for date: {current_date}")

            # Optional: window filter (from midnight to end of day)
            commence_from = current_date.strftime("%Y-%m-%dT00:00:00Z")
            commence_to = current_date.strftime("%Y-%m-%dT23:59:59Z")

            # Fetch events data
            events = theoddsapi.get_events(
                sport="basketball_nba",
                date_format="iso",
                commence_time_from=commence_from,
                commence_time_to=commence_to,
            )

            if events is None:
                print(f"No events data received for {current_date}")
                current_date += timedelta(days=1)
                continue

            if len(events) == 0:
                print(f"No events found for {current_date}")
                current_date += timedelta(days=1)
                continue

            # Convert to NDJSON
            ndjson_data = smartbetting.convert_to_ndjson(events)

            # Upload NDJSON data to Google Cloud Storage
            gcs_blob_name = (
                f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{current_date.strftime('%Y-%m-%d')}.json"
            )
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

            print(
                f"Successfully processed and uploaded {len(events)} events for {current_date}"
            )
            print(f"File saved as: {gcs_blob_name}")

            total_events_processed += len(events)
            total_requests_made += 1

            current_date += timedelta(days=1)

        print("\nPipeline completed!")
        print(f"Total events processed: {total_events_processed}")
        print(f"Total API requests made: {total_requests_made}")

    except Exception as e:
        print(f"Error in current events data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()


