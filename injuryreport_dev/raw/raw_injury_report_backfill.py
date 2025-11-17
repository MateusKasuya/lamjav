"""
Raw Injury Report data pipeline script - BACKFILL VERSION.

This script processes PDF injury reports from Google Cloud Storage for a date range
from October 20, 2025 until today and uploads the extracted data to BigQuery.
"""

import sys
import os
from typing import NoReturn
from datetime import datetime, date, timedelta

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def generate_date_range(start_date: date, end_date: date) -> list[str]:
    """
    Generate a list of dates in YYYY-MM-DD format between start and end dates.

    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)

    Returns:
        List of date strings in YYYY-MM-DD format
    """
    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    return date_list


def main() -> NoReturn:
    """
    Main function to execute the raw injury report data pipeline BACKFILL.

    This function:
    1. Defines date range from October 20, 2025 to today
    2. Lists PDF files from Google Cloud Storage for the entire date range
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

    # Define date range: from October 20, 2025 to today
    start_date = date(2025, 10, 20)
    end_date = date.today()

    # Generate list of dates to process
    target_dates = generate_date_range(start_date, end_date)

    print("=" * 80)
    print("🔄 BACKFILL: Raw Injury Report Data Pipeline")
    print("=" * 80)
    print(f"📅 Start date: {start_date.strftime('%Y-%m-%d')}")
    print(f"📅 End date: {end_date.strftime('%Y-%m-%d')}")
    print(f"📊 Total days to process: {len(target_dates)}")
    print(f"🎯 Date range: {target_dates[0]} to {target_dates[-1]}")
    print(
        f"⏰ Transformation date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (current)"
    )
    print("=" * 80)
    print()

    try:
        # Process PDFs for the entire date range
        success = smartbetting.process_injury_report_pdfs(
            bucket_name=str(bucket),
            catalog=str(catalog),
            table=str(table),
            season=str(season),
            project_id=project_id,
            target_dates=target_dates,  # All dates from Oct 20 to today
        )

        print()
        print("=" * 80)
        if success:
            print("✅ BACKFILL completed successfully")
            print(f"📅 Processed date range: {start_date} to {end_date}")
            print(f"📊 Total days processed: {len(target_dates)}")
        else:
            print("❌ BACKFILL failed")
            print("💡 No PDFs found for date range or processing failed")
        print("=" * 80)

    except Exception as e:
        print()
        print("=" * 80)
        print(f"❌ Error in backfill data pipeline: {str(e)}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
