"""
Historical NBA Injury Reports fetcher and uploader.

This script fetches multiple NBA injury reports within a date range
and uploads them to Google Cloud Storage.
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta
import argparse
from typing import List, Tuple

# Add lib_dev to path
sys.path.append(str(Path(__file__).parent.parent.parent / "lib_dev"))

from injuryreport import NBAInjuryReport, ValidationError
from smartbetting import SmartbettingLib
from utils import Bucket, Catalog, Schema


def parse_arguments():
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Fetch historical NBA injury reports and upload to GCS"
    )

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format (e.g., 2025-04-01)",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date in YYYY-MM-DD format (e.g., 2025-04-07)",
    )

    parser.add_argument(
        "--times",
        type=str,
        nargs="*",
        default=None,
        help="Report times to try (e.g., 6AM 6PM 12PM). Format: HOURPERIOD. If not specified, tries ALL hours (1AM-12AM, 1PM-12PM)",
    )

    return parser.parse_args()


def validate_date_string(date_string: str) -> date:
    """
    Validate and parse date string.

    Args:
        date_string: Date string in YYYY-MM-DD format

    Returns:
        date: Parsed date object

    Raises:
        ValueError: If date format is invalid
    """
    try:
        return datetime.strptime(date_string, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Invalid date format. Use YYYY-MM-DD. Error: {str(e)}")


def parse_time_strings(time_strings: List[str]) -> List[Tuple[int, str]]:
    """
    Parse time strings into hour and period tuples.

    Args:
        time_strings: List of time strings (e.g., ["6AM", "6PM", "12PM"])

    Returns:
        List of (hour, period) tuples

    Raises:
        ValueError: If time format is invalid
    """
    times = []

    for time_str in time_strings:
        time_str = time_str.upper().strip()

        # Extract period (AM/PM)
        if time_str.endswith("AM"):
            period = "AM"
            hour_str = time_str[:-2]
        elif time_str.endswith("PM"):
            period = "PM"
            hour_str = time_str[:-2]
        else:
            raise ValueError(f"Invalid time format: {time_str}. Must end with AM or PM")

        # Extract hour
        try:
            hour = int(hour_str)
            if not (1 <= hour <= 12):
                raise ValueError(f"Hour must be between 1 and 12, got {hour}")
        except ValueError as e:
            raise ValueError(f"Invalid hour in time: {time_str}. Error: {str(e)}")

        times.append((hour, period))

    return times


def generate_all_hours() -> List[Tuple[int, str]]:
    """
    Generate all possible hours (1AM-12AM, 1PM-12PM).

    Returns:
        List of all 24 hour combinations as (hour, period) tuples
    """
    all_hours = []

    # AM hours: 12AM, 1AM, 2AM, ..., 11AM
    for hour in [12] + list(range(1, 12)):
        all_hours.append((hour, "AM"))

    # PM hours: 12PM, 1PM, 2PM, ..., 11PM
    for hour in [12] + list(range(1, 12)):
        all_hours.append((hour, "PM"))

    return all_hours


def main():
    """
    Main function to fetch historical injury reports and upload to GCS.
    """
    print("=== NBA Historical Injury Reports Fetcher ===")
    print(f"Started at: {datetime.now()}")

    try:
        # Parse arguments
        args = parse_arguments()

        # Validate dates
        start_date = validate_date_string(args.start_date)
        end_date = validate_date_string(args.end_date)

        # Parse times - if not specified, use all hours
        if args.times is None:
            times = generate_all_hours()
            times_description = "ALL hours (1AM-12AM, 1PM-12PM)"
        else:
            times = parse_time_strings(args.times)
            times_description = args.times

        print(f"\nTarget date range: {start_date} to {end_date}")
        print(f"Times to try: {times_description}")
        print(f"Total time combinations per day: {len(times)}")

        # Initialize clients
        print("\n1. Initializing clients...")
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Fetch and upload historical injury reports (streaming approach)
        print("\n2. Fetching and uploading historical injury reports...")
        print("Using streaming approach: fetch → upload → free memory")

        total_fetched = 0
        successful_uploads = 0
        failed_uploads = 0
        failed_fetches = 0

        current_date = start_date

        while current_date <= end_date:
            print(f"\n📅 Processing date: {current_date}")
            date_fetched = 0
            date_uploaded = 0

            for hour, period in times:
                try:
                    # Fetch single report
                    result = injury_client.fetch_specific_report(
                        report_date=current_date, hour=hour, period=period
                    )

                    if result is None:
                        failed_fetches += 1
                        continue

                    pdf_data, filename = result
                    total_fetched += 1
                    date_fetched += 1

                    # Immediate upload to GCS
                    try:
                        gcs_blob_name = f"{str(Catalog.INJURY_REPORT)}/{str(Schema.LANDING)}/{filename}"

                        storage_client.upload_pdf_to_gcs(
                            pdf_data=pdf_data,
                            bucket_name=Bucket.LAMJAV_STORAGE,
                            blob_name=gcs_blob_name,
                        )

                        successful_uploads += 1
                        date_uploaded += 1
                        print(
                            f"  ✅ {hour:02d}{period}: Fetched & uploaded ({len(pdf_data)} bytes)"
                        )

                        # Free memory immediately
                        del pdf_data

                    except Exception as upload_error:
                        failed_uploads += 1
                        print(
                            f"  ❌ {hour:02d}{period}: Fetched but upload failed - {str(upload_error)}"
                        )
                        continue

                except Exception as fetch_error:
                    failed_fetches += 1
                    print(f"  ⚠️ {hour:02d}{period}: Fetch failed - {str(fetch_error)}")
                    continue

            print(
                f"  📊 Date summary: {date_fetched} fetched, {date_uploaded} uploaded"
            )
            current_date += timedelta(days=1)

        print("\n📊 Final Summary:")
        print(f"   📥 Total fetched: {total_fetched}")
        print(f"   ✅ Successful uploads: {successful_uploads}")
        print(f"   ❌ Failed uploads: {failed_uploads}")
        print(f"   ⚠️  Failed fetches: {failed_fetches}")
        print(
            f"   📈 Success rate: {(successful_uploads / max(total_fetched, 1) * 100):.1f}%"
        )

        if successful_uploads > 0:
            print("✅ Historical injury reports processing completed!")
        else:
            print("❌ No reports were successfully uploaded")

        print(f"Completed at: {datetime.now()}")
        return successful_uploads > 0

    except ValidationError as e:
        print(f"❌ Validation error: {str(e)}")
        return False
    except ValueError as e:
        print(f"❌ Value error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Error during processing: {str(e)}")
        print(f"Failed at: {datetime.now()}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
