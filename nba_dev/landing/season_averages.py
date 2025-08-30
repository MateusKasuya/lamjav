"""
NBA Season Averages data pipeline script.

This script fetches NBA season averages data from the Balldontlie API for all
category/type combinations and uploads it to Google Cloud Storage in the landing layer.
"""

import sys
import os
import time
from typing import NoReturn

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA season averages data pipeline.

    This function:
    1. Fetches season averages data for all category/type combinations
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
    table = Table.SEASON_AVERAGES
    season = Season.SEASON_2024

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    # Define all combinations
    combinations = [
        # General category
        ("general", "base"),
        ("general", "advanced"),
        ("general", "usage"),
        ("general", "scoring"),
        ("general", "defense"),
        ("general", "misc"),
        # Clutch category
        ("clutch", "advanced"),
        ("clutch", "base"),
        ("clutch", "misc"),
        ("clutch", "scoring"),
        ("clutch", "usage"),
        # Defense category
        ("defense", "2_pointers"),
        ("defense", "3_pointers"),
        ("defense", "greater_than_15ft"),
        ("defense", "less_than_10ft"),
        ("defense", "less_than_6ft"),
        ("defense", "overall"),
        # Shooting category
        ("shooting", "5ft_range"),
        ("shooting", "by_zone"),
    ]

    # Define season types for each category
    def get_season_types_for_category(category: str) -> list:
        if category == "general":
            return ["regular", "playoffs", "ist", "playin"]
        elif category in ["clutch", "defense", "shooting"]:
            return ["regular", "playoffs", "ist"]
        else:
            return ["regular", "playoffs", "ist"]

    try:
        total_successful = 0
        total_failed = 0
        total_combinations = 0

        print(f"Starting NBA season averages data pipeline for season {season}")
        print(f"Total combinations to process: {len(combinations)}")
        print("=" * 80)

        for category, type_param in combinations:
            season_types = get_season_types_for_category(category)
            category_total = len(season_types)
            total_combinations += category_total

            print(f"\n{'=' * 60}")
            print(f"PROCESSING {category.upper()} CATEGORY")
            print(f"{'=' * 60}")
            print(f"Category: {category}")
            print(f"Type: {type_param}")
            print(f"Season Types: {season_types}")
            print(f"Total combinations for this category: {category_total}")

            category_successful = 0
            category_failed = 0

            for season_type in season_types:
                try:
                    print(
                        f"\nProcessing: {category}/{type_param}/{season_type}/{season}"
                    )

                    # Fetch season averages data from API
                    response = balldontlie.get_season_averages(
                        category, season_type, type_param, season
                    )

                    if response is None or len(response) == 0:
                        print(
                            f"No data received for {category}/{type_param}/{season_type}/{season}"
                        )
                        category_failed += 1
                        continue

                    # Convert API response to dictionary format
                    data = smartbetting.convert_object_to_dict(response)

                    # Convert data to NDJSON format for BigQuery compatibility
                    ndjson_data = smartbetting.convert_to_ndjson(data)

                    # Generate storage path and blob name
                    storage_path = f"{catalog}/{table}/{category}/{type_param}/{season_type}/{season}"
                    gcs_blob_name = f"{storage_path}/raw_{catalog}_{table}_{category}_{type_param}_{season_type}_{season}.json"

                    # Upload NDJSON data to Google Cloud Storage
                    smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

                    print(
                        f"✅ Successfully uploaded {len(data)} records for {category}/{type_param}/{season_type}/{season}"
                    )
                    category_successful += 1

                except Exception as e:
                    print(
                        f"❌ Error processing {category}/{type_param}/{season_type}/{season}: {str(e)}"
                    )
                    category_failed += 1

                # Add delay between API calls to respect rate limits
                time.sleep(2)

            # Print category summary
            print(f"\n📊 {category.upper()} CATEGORY SUMMARY:")
            print(f"✅ Successful: {category_successful}")
            print(f"❌ Failed: {category_failed}")
            print(f"📊 Total: {category_total}")

            total_successful += category_successful
            total_failed += category_failed

        # Print overall summary
        print("\n" + "=" * 80)
        print("OVERALL EXTRACTION SUMMARY:")
        print(f"✅ Total successful extractions: {total_successful}")
        print(f"❌ Total failed extractions: {total_failed}")
        print(f"📊 Total combinations processed: {total_combinations}")

        if total_successful > 0:
            print(
                f"\n🎉 Successfully extracted {total_successful} season averages datasets across all categories!"
            )
        else:
            print(
                "\n⚠️  No successful extractions. Check API configuration and parameters."
            )

    except Exception as e:
        print(f"Error in season averages data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
