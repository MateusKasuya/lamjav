"""
Specific NBA Injury Report fetcher and uploader.

This script fetches a specific NBA injury report by date and time,
and uploads it to Google Cloud Storage.
"""

import sys
from pathlib import Path
from datetime import datetime, date
import argparse

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
        description="Fetch specific NBA injury report and upload to GCS"
    )

    parser.add_argument(
        "--date",
        type=str,
        required=True,
        help="Report date in YYYY-MM-DD format (e.g., 2025-04-07)",
    )

    parser.add_argument(
        "--hour",
        type=int,
        required=True,
        choices=range(1, 13),
        help="Report hour (1-12)",
    )

    parser.add_argument(
        "--period",
        type=str,
        required=True,
        choices=["AM", "PM"],
        help="Report period (AM or PM)",
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


def main():
    """
    Main function to fetch specific injury report and upload to GCS.
    """
    print("=== NBA Specific Injury Report Fetcher ===")
    print(f"Started at: {datetime.now()}")

    try:
        # Parse arguments
        args = parse_arguments()

        # Validate date
        report_date = validate_date_string(args.date)

        print(f"\nTarget report: {report_date} at {args.hour:02d}{args.period}")

        # Initialize clients
        print("\n1. Initializing clients...")
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Fetch specific injury report
        print(
            f"\n2. Fetching injury report for {report_date} at {args.hour:02d}{args.period}..."
        )
        result = injury_client.fetch_specific_report(
            report_date=report_date, hour=args.hour, period=args.period
        )

        if result is None:
            print(
                f"❌ Failed to fetch injury report for {report_date} at {args.hour:02d}{args.period}"
            )
            return False

        pdf_data, filename = result
        print(f"✅ Successfully fetched: {filename} ({len(pdf_data)} bytes)")

        # Prepare GCS path using report date/time from filename
        # Extract date and time from filename: injury_report_YYYY-MM-DD_HHPERIOD.pdf
        gcs_blob_name = f"{str(Catalog.INJURY_REPORT)}/{str(Schema.LANDING)}/{filename}"

        print("\n3. Uploading to GCS...")
        print(f"Bucket: {Bucket.LAMJAV_STORAGE}")
        print(f"Blob path: {gcs_blob_name}")

        # Upload to GCS
        storage_client.upload_pdf_to_gcs(
            pdf_data=pdf_data,
            bucket_name=Bucket.LAMJAV_STORAGE,
            blob_name=gcs_blob_name,
        )

        print("✅ Specific injury report processing completed successfully!")
        print(f"Completed at: {datetime.now()}")
        return True

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
