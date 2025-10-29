"""
[OPTIONAL] Extract current Event IDs and Commence Times from Events data.

⚠️  NOTE: This script is OPTIONAL and creates intermediate storage.
    For Cloud Functions/Cloud Run deployments, it's MORE COST-EFFECTIVE to read
    directly from EVENTS (as event_odds.py and historical_event_odds.py do).

This script reads event IDs from the events snapshot and saves them separately.
Useful for debugging or if you need a lightweight event ID reference.

Cost-Benefit Analysis:
✅ Direct read from EVENTS: 3 function invocations, simpler workflow
❌ Using this script: 4 function invocations, extra complexity

Path Structure:
- Bucket: SMARTBETTING_STORAGE
- Season: SEASON_2025
- Input Path: odds/events/season_2025/
- Output Path: odds/event_id/season_2025/
"""

import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Table, Season


def main():
    """
    Main function to extract and save current event IDs and commence times.

    This function:
    1. Clears the output folder to remove old data
    2. Reads event IDs from each events file separately
    3. Saves each set of event IDs with the date from the source filename

    Note: Each events file is processed independently, preserving the date
    from the source filename in the output filename.
    """
    # Initialize API client
    smartbetting = SmartbettingLib()

    # Configuration
    bucket = Bucket.SMARTBETTING_STORAGE
    catalog = Catalog.ODDS
    season = Season.SEASON_2025
    input_table = Table.EVENTS
    output_table = Table.EVENT_ID

    print("=" * 60)
    print("🎯 EXTRACTING CURRENT EVENT IDs")
    print("=" * 60)

    # Clean up old data from output folder
    output_prefix = f"{catalog}/{output_table}/{season}/"
    print(f"\n🗑️  Cleaning old data from: {output_prefix}")
    smartbetting.delete_gcs_folder_contents(
        bucket_name=str(bucket),
        prefix=output_prefix,
    )

    # List all events files
    print("\n📂 Listing events files...")
    events_files = smartbetting.list_events_files(
        bucket_name=str(bucket),
        catalog=str(catalog),
        table=str(input_table),
        season=str(season),
    )

    if not events_files:
        print("❌ No events files found")
        return

    print(f"Found {len(events_files)} events file(s)")

    # Process each file separately
    total_files_processed = 0
    total_events_processed = 0

    for file_name in events_files:
        print(f"\n{'─' * 60}")
        print(f"📄 Processing file: {file_name.split('/')[-1]}")

        # Extract date from filename
        file_date = smartbetting.extract_date_from_filename(file_name)

        if not file_date:
            print("⚠️  Could not extract date from filename, skipping...")
            continue

        print(f"📅 Extracted date: {file_date}")

        # Extract event IDs from this file
        event_data = smartbetting.extract_event_ids_from_single_file(
            bucket_name=str(bucket),
            file_name=file_name,
        )

        if not event_data:
            print("⚠️  No events found in this file")
            continue

        print(f"Found {len(event_data)} event(s)")

        # Save event IDs to storage with the date from source file
        saved_path = smartbetting.save_event_ids_to_storage(
            event_data=event_data,
            bucket_name=str(bucket),
            catalog=str(catalog),
            table=str(output_table),
            season=str(season),
            date_suffix=file_date,
        )

        if saved_path:
            print(f"✅ Saved to: {saved_path}")
            total_files_processed += 1
            total_events_processed += len(event_data)
        else:
            print("❌ Failed to save event data")

    # Final summary
    print("\n" + "=" * 60)
    print("✅ EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"📁 Files processed: {total_files_processed}/{len(events_files)}")
    print(f"🎯 Total events processed: {total_events_processed}")
    print(f"📂 Output location: {output_prefix}")


if __name__ == "__main__":
    main()
