"""
NBA Injury Report data pipeline script.

This script fetches the most recent NBA injury report and uploads
it to Google Cloud Storage in the landing layer of the data lake.
"""

import sys
import os
from typing import NoReturn

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.injuryreport import NBAInjuryReport
from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the NBA injury report data pipeline.

    This function:
    1. Fetches the most recent injury report from NBA API
    2. Extracts report information (date, time, period)
    3. Uploads the PDF to Google Cloud Storage in the landing layer
    4. Includes season parameter for organizational consistency

    Returns:
        None

    Raises:
        Exception: For any unexpected errors during execution
    """
    # Initialize constants
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.INJURY_REPORT
    table = Table.INJURY_REPORT
    season = Season.SEASON_2025  # Season for organizational purposes

    # Initialize API clients
    injury_client = NBAInjuryReport()
    smartbetting = SmartbettingLib()

    try:
        print("Starting injury report data pipeline")
        print(f"Season: {season} (for organization)")
        print("=" * 80)

        # Fetch current injury report
        result = injury_client.fetch_current_report()

        if result is None:
            raise Exception("Failed to fetch current injury report from NBA API")

        pdf_data, filename = result

        # Extract report information for logging
        report_info = injury_client._extract_report_info(filename)

        print(
            f"✅ Successfully fetched injury report: {filename} ({len(pdf_data)} bytes)"
        )
        print(f"📅 Report date: {report_info['date']}")
        print(f"🕐 Report time: {report_info['time']}")
        print(f"📊 Period: {report_info['period']}")

        # Upload PDF to Google Cloud Storage
        # Note: Injury reports are dynamic data, stored with season for organization
        gcs_blob_name = f"{catalog}/{table}/{season}/{filename}"
        success = smartbetting.upload_pdf_to_gcs(pdf_data, bucket, gcs_blob_name)

        if not success:
            raise Exception("Failed to upload injury report to Google Cloud Storage")

        print("✅ Successfully uploaded injury report to Google Cloud Storage")
        print(f"📁 Stored in: gs://{bucket}/{gcs_blob_name}")
        print(f"🎯 Season: {season} (organizational)")

    except Exception as e:
        print(f"Error in injury report data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
