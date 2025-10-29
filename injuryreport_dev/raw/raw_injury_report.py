"""
Raw Injury Report data pipeline script.

This script processes PDF injury reports from Google Cloud Storage for the current day
and uploads the extracted data to BigQuery. The transformation date is always current.
"""

import sys
import os
from typing import NoReturn
from datetime import datetime, date

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main() -> NoReturn:
    """
    Main function to execute the raw injury report data pipeline.

    This function:
    1. Gets today's date
    2. Lists PDF files from Google Cloud Storage for today only
    3. Downloads and extracts data from each PDF
    4. Inserts all data into BigQuery at once
    5. Clears memory completely

    Raises:
        ValueError: If environment variables are not configured
        Exception: For any other unexpected errors during execution
    """
    # Load environment variables
    from dotenv import load_dotenv

    load_dotenv()

    # Initialize constants
    project_id = os.getenv("DBT_PROJECT")
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.INJURY_REPORT
    table = Table.INJURY_REPORT
    season = Season.SEASON_2025  # Season for organizational consistency

    # Initialize API client
    smartbetting = SmartbettingLib()

    # Validation
    if not project_id:
        raise ValueError("Environment variable DBT_PROJECT is not configured")

    # Get today's date
    today = date.today()
    today_str = today.strftime("%Y-%m-%d")

    print("Starting raw injury report data pipeline")
    print(f"Processing date: {today_str} (today only)")
    print(
        f"Transformation date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (always current)"
    )
    print("=" * 80)

    try:
        # Process PDFs for today only
        success = smartbetting.process_injury_report_pdfs(
            bucket_name=str(bucket),
            catalog=str(catalog),
            table=str(table),
            season=str(season),
            project_id=project_id,
            target_dates=[today_str],  # Only today's files
        )

        if success:
            print("✅ Raw injury report processing completed successfully")
            print(f"📅 Processed files from: {today_str}")
        else:
            print("❌ Raw injury report processing failed")
            print(f"💡 No PDFs found for today ({today_str}) or processing failed")

    except Exception as e:
        print(f"Error in raw injury report data pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
