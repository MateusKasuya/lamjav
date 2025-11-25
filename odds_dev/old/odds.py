"""
The Odds API NBA Odds data pipeline script.

This script fetches NBA odds data from The Odds API for both single-day
and date range extractions, then uploads it to Google Cloud Storage in the landing layer.
Includes proper rate limiting, error handling, and cost optimization.
"""

import sys
import os
import time
from typing import NoReturn
from datetime import date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.theoddsapi import TheOddsAPILib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the The Odds API NBA odds data pipeline.

    This function:
    1. Fetches NBA odds data from The Odds API for today's games
    2. Optionally fetches historical data for a date range
    3. Converts the data to the required format
    4. Uploads the data to Google Cloud Storage in the landing layer
    5. Includes cost tracking and rate limiting
    6. Provides detailed summary reporting

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    table = Table.ODDS
    season = Season.SEASON_2024

    # Define markets for extraction (optimized for cost)
    markets = "totals"  # 3 markets = 3 credits per request

    # Set date range for extraction (adjust as needed)
    start_date = date.today()  # Today only, or set to historical date
    end_date = date.today()  # For single day, or extend for range

    # Initialize API clients
    theoddsapi = TheOddsAPILib()
    smartbetting = SmartbettingLib()

    try:
        print("Starting NBA Odds Data Pipeline")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Markets: {markets}")
        print("=" * 80)

        total_events_processed = 0
        total_requests_made = 0
        current_date = start_date

        while current_date <= end_date:
            print(f"\nProcessing odds for date: {current_date}")

            # Fetch NBA odds data from API
            # Using optimized markets for cost efficiency
            response = theoddsapi.get_odds(
                sport="basketball_nba",
                regions="us",
                markets=markets,
                odds_format="decimal",  # Using decimal format for consistency
                # Optional: filter by date range
                # commence_time_from=current_date.strftime("%Y-%m-%dT00:00:00Z"),
                # commence_time_to=current_date.strftime("%Y-%m-%dT23:59:59Z"),
            )

            if response is None or len(response) == 0:
                print(f"No odds data received for {current_date}")
                current_date += timedelta(days=1)
                continue

            # The Odds API returns native Python dictionaries
            data = response

            # Convert data to NDJSON format for BigQuery compatibility
            ndjson_data = smartbetting.convert_to_ndjson(data)

            # Upload NDJSON data to GCS with date in filename
            gcs_key = f"{catalog}/{table}/{season}/raw_{catalog}_{table}_{season}.json"
            smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_key)

            print(
                f"✅ Successfully processed and uploaded {len(data)} NBA odds events for {current_date}"
            )
            print(f"📁 Saved to: {gcs_key}")

            total_events_processed += len(data)
            total_requests_made += 1

            # Move to next date
            current_date += timedelta(days=1)

            # Add delay between requests to respect rate limits
            if current_date <= end_date:
                print("Waiting 2 seconds before next request...")
                time.sleep(2)

        # Print overall summary
        print("\n" + "=" * 80)
        print("NBA ODDS EXTRACTION SUMMARY:")
        print(f"📅 Date range: {start_date} to {end_date}")
        print(f"🎯 Total events processed: {total_events_processed}")
        print(f"📞 Total API requests made: {total_requests_made}")
        print(
            f"💰 Estimated credits used: {total_requests_made * 3} (3 credits per request)"
        )
        print(f"🏷️  Markets extracted: {markets}")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error in NBA odds data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
