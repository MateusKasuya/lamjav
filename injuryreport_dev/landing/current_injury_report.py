"""
Current NBA Injury Report fetcher and uploader.

This script fetches the current NBA injury report and uploads it to Google Cloud Storage.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add lib_dev to path
sys.path.append(str(Path(__file__).parent.parent.parent / "lib_dev"))

from injuryreport import NBAInjuryReport
from smartbetting import SmartbettingLib
from utils import Bucket, Catalog, Schema


def main():
    """
    Main function to fetch current injury report and upload to GCS.
    """
    print("=== NBA Current Injury Report Fetcher ===")
    print(f"Started at: {datetime.now()}")

    try:
        # Initialize clients
        print("\n1. Initializing clients...")
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Fetch current injury report
        print("\n2. Fetching current injury report...")
        result = injury_client.fetch_current_report()

        if result is None:
            print("❌ Failed to fetch current injury report")
            return False

        pdf_data, filename = result
        print(f"✅ Successfully fetched: {filename} ({len(pdf_data)} bytes)")

        # Prepare GCS path using report date/time from filename
        # Extract date and time from filename: current_injury_report_YYYY-MM-DD_HHPERIOD.pdf
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

        print("✅ Current injury report processing completed successfully!")
        print(f"Completed at: {datetime.now()}")
        return True

    except Exception as e:
        print(f"❌ Error during processing: {str(e)}")
        print(f"Failed at: {datetime.now()}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
