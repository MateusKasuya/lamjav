"""
Example script showing how to run all injury report collections.

This is a demonstration script that shows how to use all three
injury report collection methods in sequence.
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta

# Add lib_dev to path
sys.path.append(str(Path(__file__).parent.parent.parent / "lib_dev"))

from injuryreport import NBAInjuryReport
from smartbetting import SmartbettingLib
from utils import Bucket


def run_current_report():
    """
    Example: Fetch and upload current injury report.
    """
    print("=== EXAMPLE 1: Current Injury Report ===")

    try:
        # Initialize clients
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Fetch current report
        result = injury_client.fetch_current_report()

        if result is None:
            print("❌ No current report available")
            return False

        pdf_data, filename = result
        print(f"✅ Fetched: {filename} ({len(pdf_data)} bytes)")

        # Upload to GCS
        gcs_blob_name = f"injury_report/landing/{filename}"

        storage_client.upload_pdf_to_gcs(
            pdf_data=pdf_data,
            bucket_name=Bucket.LAMJAV_STORAGE,
            blob_name=gcs_blob_name,
        )

        print("✅ Current report uploaded successfully")
        return True

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False


def run_specific_report():
    """
    Example: Fetch and upload a specific injury report.
    """
    print("\n=== EXAMPLE 2: Specific Injury Report ===")

    try:
        # Initialize clients
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Example: Get report from 3 days ago at 6 PM
        target_date = date.today() - timedelta(days=3)

        # Fetch specific report
        result = injury_client.fetch_specific_report(
            report_date=target_date, hour=6, period="PM"
        )

        if result is None:
            print(f"❌ No report available for {target_date} at 6 PM")
            return False

        pdf_data, filename = result
        print(f"✅ Fetched: {filename} ({len(pdf_data)} bytes)")

        # Upload to GCS
        gcs_blob_name = f"injury_report/landing/{filename}"

        storage_client.upload_pdf_to_gcs(
            pdf_data=pdf_data,
            bucket_name=Bucket.LAMJAV_STORAGE,
            blob_name=gcs_blob_name,
        )

        print("✅ Specific report uploaded successfully")
        return True

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False


def run_historical_reports():
    """
    Example: Fetch and upload historical injury reports.
    """
    print("\n=== EXAMPLE 3: Historical Injury Reports ===")

    try:
        # Initialize clients
        injury_client = NBAInjuryReport()
        storage_client = SmartbettingLib()

        # Example: Get reports from last 3 days
        end_date = date.today() - timedelta(days=1)  # Yesterday
        start_date = end_date - timedelta(days=2)  # 3 days total

        print(f"Fetching reports from {start_date} to {end_date}")

        # Fetch historical reports
        fetched_reports = injury_client.fetch_historical_reports(
            start_date=start_date,
            end_date=end_date,
            times=[(6, "AM"), (6, "PM")],  # Try both morning and evening reports
        )

        if not fetched_reports:
            print("❌ No historical reports available")
            return False

        print(f"✅ Fetched {len(fetched_reports)} historical reports")

        # Upload each report to GCS
        successful_uploads = 0
        for pdf_data, filename in fetched_reports:
            try:
                # Prepare GCS path using report date/time from filename
                gcs_blob_name = f"injury_report/landing/{filename}"

                storage_client.upload_pdf_to_gcs(
                    pdf_data=pdf_data,
                    bucket_name=Bucket.LAMJAV_STORAGE,
                    blob_name=gcs_blob_name,
                )

                successful_uploads += 1

            except Exception as e:
                print(f"❌ Failed to upload {filename}: {str(e)}")
                continue

        print(
            f"✅ Successfully uploaded {successful_uploads}/{len(fetched_reports)} historical reports"
        )
        return successful_uploads > 0

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False


def main():
    """
    Main function - run all examples.
    """
    print("🏀 NBA Injury Reports - Complete Example")
    print(f"Started at: {datetime.now()}")
    print("=" * 60)

    results = []

    # Run all examples
    results.append(run_current_report())
    results.append(run_specific_report())
    results.append(run_historical_reports())

    # Summary
    print("\n" + "=" * 60)
    print("📊 SUMMARY:")
    print(f"  Current report:     {'✅ Success' if results[0] else '❌ Failed'}")
    print(f"  Specific report:    {'✅ Success' if results[1] else '❌ Failed'}")
    print(f"  Historical reports: {'✅ Success' if results[2] else '❌ Failed'}")
    print(f"  Overall success:    {sum(results)}/3 operations")
    print(f"Completed at: {datetime.now()}")

    return all(results)


if __name__ == "__main__":
    success = main()
    print(f"\n🎯 Exit status: {'SUCCESS' if success else 'PARTIAL_SUCCESS'}")
    sys.exit(0)  # Always exit 0 for examples
