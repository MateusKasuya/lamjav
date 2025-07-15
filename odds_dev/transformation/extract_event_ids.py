"""
Extract Event IDs from Historical Events Data.

This script reads historical events data from Google Cloud Storage
in the odds/landing/historical_events folder and extracts all event IDs
from all files, then saves them to a single file in storage at
odds/transformation/historical_event_id/all_event_ids.json.
"""

import sys
import os
from typing import List, Set, Dict, Any
from datetime import date, datetime, timedelta
import json

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.smartbetting import SmartbettingLib
from lib_dev.utils import Bucket, Catalog, Schema, Table


class EventIDExtractor:
    """
    Class to extract event IDs from historical events data stored in GCS.
    """

    def __init__(self):
        """Initialize the extractor with SmartbettingLib for GCS operations."""
        self.smartbetting = SmartbettingLib()
        self.bucket = Bucket.LAMJAV_STORAGE
        self.catalog = Catalog.ODDS
        self.schema = Schema.LANDING
        self.table = Table.HISTORICAL_EVENTS
        # Output path for saving event IDs
        self.output_catalog = Catalog.ODDS
        self.output_schema = "transformation"
        self.output_table = "historical_event_id"

    def list_historical_events_files(self) -> List[str]:
        """
        List all historical events files in the GCS folder.

        Returns:
            List of file names (blob names) in the historical_events folder
        """
        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(str(self.bucket))

            # List all blobs in the historical_events folder
            prefix = f"{self.catalog}/{self.schema}/{self.table}/"
            blobs = bucket.list_blobs(prefix=prefix)

            file_names = []
            for blob in blobs:
                if blob.name.endswith(".json"):
                    file_names.append(blob.name)

            print(f"Found {len(file_names)} historical events files")
            return file_names

        except Exception as e:
            print(f"Error listing files: {e}")
            return []

    def read_historical_events_file(self, file_name: str) -> List[Dict[str, Any]]:
        """
        Read a single historical events file from GCS.

        Args:
            file_name: The GCS blob name to read

        Returns:
            List of event dictionaries from the file
        """
        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(str(self.bucket))
            blob = bucket.blob(file_name)

            # Download and parse JSON content
            content = blob.download_as_text()
            events_data = json.loads(content)

            print(f"Read {len(events_data)} events from {file_name}")
            return events_data

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            return []

    def extract_all_event_ids(self) -> Set[str]:
        """
        Extract all event IDs from all historical events files.

        Returns:
            Set of unique event IDs from all files
        """
        print("Extracting event IDs from all historical events files...")

        # List all files
        file_names = self.list_historical_events_files()
        if not file_names:
            print("No historical events files found")
            return set()

        # Extract event IDs from all files
        event_ids = set()
        total_events = 0
        processed_files = 0

        for file_name in file_names:
            events_data = self.read_historical_events_file(file_name)

            file_event_count = 0
            for event in events_data:
                event_id = event.get("id")
                if event_id:
                    event_ids.add(event_id)
                    file_event_count += 1
                    total_events += 1

            processed_files += 1
            print(
                f"Processed file {processed_files}/{len(file_names)}: {file_event_count} events"
            )

        print(
            f"Extracted {len(event_ids)} unique event IDs from {total_events} total events across {processed_files} files"
        )
        return event_ids

    def get_event_details(self, event_ids: Set[str]) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed information for specific event IDs.

        Args:
            event_ids: Set of event IDs to get details for

        Returns:
            Dictionary mapping event ID to event details
        """
        print(f"Getting details for {len(event_ids)} event IDs...")

        file_names = self.list_historical_events_files()
        event_details = {}

        for file_name in file_names:
            events_data = self.read_historical_events_file(file_name)

            for event in events_data:
                event_id = event.get("id")
                if event_id in event_ids and event_id not in event_details:
                    event_details[event_id] = event

        print(f"Found details for {len(event_details)} events")
        return event_details

    def save_all_event_ids_to_storage(self, event_ids: Set[str]) -> str:
        """
        Save all extracted event IDs to a single file in Google Cloud Storage.

        Args:
            event_ids: Set of all event IDs to save

        Returns:
            The GCS blob name where the file was saved
        """
        # Create filename: all_event_ids.json
        filename = "all_event_ids.json"

        # Create the full GCS path
        gcs_path = (
            f"{self.output_catalog}/{self.output_schema}/{self.output_table}/{filename}"
        )

        # Prepare data structure
        data = {
            "extraction_date": date.today().isoformat(),
            "total_unique_event_ids": len(event_ids),
            "event_ids": sorted(list(event_ids)),
            "metadata": {
                "source": "historical_events",
                "extraction_timestamp": datetime.now().isoformat(),
                "file_format": "json",
                "description": "All unique event IDs extracted from historical events data",
            },
        }

        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(str(self.bucket))
            blob = bucket.blob(gcs_path)

            # Upload the JSON data
            blob.upload_from_string(
                json.dumps(data, indent=2, ensure_ascii=False),
                content_type="application/json",
            )

            print(
                f"✅ Successfully saved {len(event_ids)} unique event IDs to: {gcs_path}"
            )
            return gcs_path

        except Exception as e:
            print(f"❌ Error saving event IDs to storage: {e}")
            return None

    def extract_and_save_all_event_ids(self) -> Set[str]:
        """
        Extract all event IDs from all files and save them to storage in one operation.

        Returns:
            Set of extracted event IDs
        """
        # Extract all event IDs
        event_ids = self.extract_all_event_ids()

        if event_ids:
            # Save to storage
            saved_path = self.save_all_event_ids_to_storage(event_ids)
            if saved_path:
                print(f"📁 All event IDs saved to: {saved_path}")
            else:
                print("⚠️  Failed to save event IDs to storage")
        else:
            print("⚠️  No event IDs to save")

        return event_ids


def main():
    """
    Main function to demonstrate event ID extraction and storage.
    """
    extractor = EventIDExtractor()

    # Extract all event IDs from all files and save to storage
    print("=" * 60)
    print("EXTRACTING ALL EVENT IDs FROM ALL FILES")
    print("=" * 60)

    all_event_ids = extractor.extract_and_save_all_event_ids()

    if all_event_ids:
        print(
            f"\n✅ Successfully extracted and saved {len(all_event_ids)} unique event IDs"
        )

        # Show sample event IDs
        sample_ids = list(all_event_ids)[:10]
        print(f"\n📋 Sample Event IDs:")
        for i, event_id in enumerate(sample_ids, 1):
            print(f"{i:2d}. {event_id}")

        if len(all_event_ids) > 10:
            print(f"   ... and {len(all_event_ids) - 10} more")

        # Get details for sample events
        print(f"\n📊 Getting details for sample events...")
        sample_details = extractor.get_event_details(set(sample_ids))

        print(f"\n📋 Sample Event Details:")
        for i, (event_id, details) in enumerate(sample_details.items(), 1):
            print(f"{i:2d}. {details.get('away_team')} @ {details.get('home_team')}")
            print(f"    ID: {event_id}")
            print(f"    Commence: {details.get('commence_time')}")
            print()

    else:
        print("❌ No event IDs found")


if __name__ == "__main__":
    main()
