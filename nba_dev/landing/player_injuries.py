"""
NBA Player Injuries data pipeline script.

This script fetches NBA player injuries data from the Balldontlie API and
uploads it to Google Cloud Storage in the landing layer.
"""

import sys
import os
from typing import NoReturn

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA player injuries data pipeline.

    This function:
    1. Fetches player injuries data from the Balldontlie API
    2. Converts the data to the required format
    3. Uploads the data to Google Cloud Storage in the landing layer
    4. Includes proper error handling and logging

    Returns:
        None

    Raises:
        ValueError: If API key is not configured
        Exception: For any other unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.NBA
    table = Table.PLAYER_INJURIES
    season = Season.SEASON_2024

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    try:
        print(f"Starting NBA player injuries data pipeline for season {season}")
        print("=" * 80)

        # Fetch player injuries data from API
        print("Fetching player injuries data...")
        response = balldontlie.get_injuries()

        if response is None:
            print("❌ No response received from API")
            return

        if len(response) == 0:
            print("⚠️  No player injuries data received")
            return

        # Convert API response to dictionary format
        data = smartbetting.convert_object_to_dict(response)

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Generate storage path and blob name
        storage_path = f"{catalog}/{table}/{season}"
        gcs_blob_name = f"{storage_path}/raw_{catalog}_{table}_{season}.json"

        # Upload NDJSON data to Google Cloud Storage
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

        print(f"✅ Successfully uploaded {len(data)} player injuries records")
        print(f"📁 Stored in: {gcs_blob_name}")

        # Show sample data structure
        if data and len(data) > 0:
            sample_record = data[0]
            print(f"📊 Sample fields: {list(sample_record.keys())}")

            # Show player info if available
            if "player" in sample_record:
                player = sample_record["player"]
                print(
                    f"👤 Sample player: {player.get('first_name', 'N/A')} {player.get('last_name', 'N/A')}"
                )
                print(f"🏥 Sample injury status: {sample_record.get('status', 'N/A')}")
                print(
                    f"📅 Sample return date: {sample_record.get('return_date', 'N/A')}"
                )

        print("\n" + "=" * 80)
        print("🎉 PLAYER INJURIES EXTRACTION COMPLETED SUCCESSFULLY!")
        print(f"📊 Total records extracted: {len(data)}")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Error in player injuries data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
