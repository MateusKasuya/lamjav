"""
NBA Season Averages data pipeline script.

This script fetches NBA season averages data from the Balldontlie API for
ALL available category/type/season_type combinations and uploads to Google Cloud Storage.
"""

import sys
import os
from typing import NoReturn, List, Tuple
from datetime import date
import time

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.balldontlie import BalldontlieLib
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table


def get_all_combinations() -> List[Tuple[str, str, str]]:
    """
    Get all valid category/type/season_type combinations based on Balldontlie API documentation.

    Returns:
        List of tuples containing (category, type, season_type) combinations
    """
    combinations = [
        # General category combinations
        ("general", "base", "regular"),
        ("general", "advanced", "regular"),
        ("general", "usage", "regular"),
        ("general", "scoring", "regular"),
        ("general", "defense", "regular"),
        ("general", "misc", "regular"),
        # General category with other season types
        ("general", "base", "playoffs"),
        ("general", "advanced", "playoffs"),
        ("general", "usage", "playoffs"),
        ("general", "scoring", "playoffs"),
        ("general", "defense", "playoffs"),
        ("general", "misc", "playoffs"),
        ("general", "base", "ist"),
        ("general", "advanced", "ist"),
        ("general", "usage", "ist"),
        ("general", "scoring", "ist"),
        ("general", "defense", "ist"),
        ("general", "misc", "ist"),
        ("general", "base", "playin"),
        ("general", "advanced", "playin"),
        ("general", "usage", "playin"),
        ("general", "scoring", "playin"),
        ("general", "defense", "playin"),
        ("general", "misc", "playin"),
        # Clutch category combinations
        ("clutch", "advanced", "regular"),
        ("clutch", "base", "regular"),
        ("clutch", "misc", "regular"),
        ("clutch", "scoring", "regular"),
        ("clutch", "usage", "regular"),
        ("clutch", "advanced", "playoffs"),
        ("clutch", "base", "playoffs"),
        ("clutch", "misc", "playoffs"),
        ("clutch", "scoring", "playoffs"),
        ("clutch", "usage", "playoffs"),
        # Defense category combinations
        ("defense", "2_pointers", "regular"),
        ("defense", "3_pointers", "regular"),
        ("defense", "greater_than_15ft", "regular"),
        ("defense", "less_than_10ft", "regular"),
        ("defense", "less_than_6ft", "regular"),
        ("defense", "overall", "regular"),
        ("defense", "2_pointers", "playoffs"),
        ("defense", "3_pointers", "playoffs"),
        ("defense", "greater_than_15ft", "playoffs"),
        ("defense", "less_than_10ft", "playoffs"),
        ("defense", "less_than_6ft", "playoffs"),
        ("defense", "overall", "playoffs"),
        # Shooting category combinations
        ("shooting", "5ft_range", "regular"),
        ("shooting", "by_zone", "regular"),
        ("shooting", "5ft_range", "playoffs"),
        ("shooting", "by_zone", "playoffs"),
    ]

    return combinations


def fetch_and_upload_season_averages(
    balldontlie: BalldontlieLib,
    smartbetting: SmartbettingLib,
    bucket: str,
    catalog: str,
    table: str,
    category: str,
    type_param: str,
    season_type: str,
    season: int,
    extraction_date: str,
) -> bool:
    """
    Fetch season averages for a specific combination and upload to GCS.

    Args:
        balldontlie: Balldontlie API client
        smartbetting: Smartbetting client for GCS operations
        bucket: GCS bucket name
        catalog: Data catalog name
        table: Table name
        category: Season averages category
        type_param: Season averages type
        season_type: Season type
        season: Season year
        extraction_date: Date of extraction

    Returns:
        True if successful, False otherwise
    """
    try:
        print(
            f"Processing: category={category}, type={type_param}, season_type={season_type}, season={season}"
        )

        # Fetch season averages data from API
        response = balldontlie.get_season_averages(
            category, season_type, type_param, season
        )

        if response is None or len(response) == 0:
            print(
                f"No data received for {category}/{type_param}/{season_type}/{season}"
            )
            return False

        # Convert API response to dictionary format (if needed)
        if response and hasattr(response[0], "model_dump"):
            data = smartbetting.convert_object_to_dict(response)
        else:
            data = response  # Already in dictionary format

        # Convert data to NDJSON format for BigQuery compatibility
        ndjson_data = smartbetting.convert_to_ndjson(data)

        # Upload NDJSON data to Google Cloud Storage
        gcs_blob_name = f"{catalog}/{table}/raw_{catalog}_{table}_{category}_{type_param}_{season_type}_{season}_{extraction_date}.json"
        smartbetting.upload_json_to_gcs(ndjson_data, bucket, gcs_blob_name)

        print(
            f"✅ Successfully uploaded {len(data)} records for {category}/{type_param}/{season_type}/{season}"
        )
        return True

    except Exception as e:
        print(
            f"❌ Error processing {category}/{type_param}/{season_type}/{season}: {str(e)}"
        )
        return False


def main() -> NoReturn:
    """
    Main function to fetch NBA season averages data for ALL combinations and upload to GCS.

    This function:
    1. Fetches season averages for ALL valid category/type/season_type combinations
    2. Uploads each combination to Google Cloud Storage with appropriate naming
    3. Handles errors gracefully and continues with other combinations

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

    # Season parameter
    season = 2024

    # Initialize API clients
    balldontlie = BalldontlieLib()
    smartbetting = SmartbettingLib()

    # Get all valid combinations
    combinations = get_all_combinations()
    extraction_date = date.today().strftime("%Y-%m-%d")

    print(f"Starting extraction of {len(combinations)} season averages combinations...")
    print(f"Season: {season}, Extraction date: {extraction_date}")
    print("=" * 80)

    successful_extractions = 0
    failed_extractions = 0

    try:
        for i, (category, type_param, season_type) in enumerate(combinations, 1):
            print(f"\n[{i}/{len(combinations)}] Processing combination...")

            success = fetch_and_upload_season_averages(
                balldontlie=balldontlie,
                smartbetting=smartbetting,
                bucket=bucket,
                catalog=catalog,
                table=table,
                category=category,
                type_param=type_param,
                season_type=season_type,
                season=season,
                extraction_date=extraction_date,
            )

            if success:
                successful_extractions += 1
            else:
                failed_extractions += 1

            # Add delay between API calls to respect rate limits
            if i < len(combinations):  # Don't sleep after the last call
                time.sleep(2)

        print("\n" + "=" * 80)
        print("EXTRACTION SUMMARY:")
        print(f"✅ Successful extractions: {successful_extractions}")
        print(f"❌ Failed extractions: {failed_extractions}")
        print(f"📊 Total combinations processed: {len(combinations)}")
        print(f"📅 Extraction date: {extraction_date}")

        if successful_extractions > 0:
            print(
                f"\n🎉 Successfully extracted {successful_extractions} season averages datasets!"
            )
        else:
            print(
                "\n⚠️  No successful extractions. Check API configuration and parameters."
            )

    except Exception as e:
        print(f"Critical error in season averages pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
