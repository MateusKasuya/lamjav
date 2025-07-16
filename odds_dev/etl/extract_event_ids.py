"""
Extract Event IDs and Commence Times from Historical Events Data.

This script reads historical events data from Google Cloud Storage
in the odds/landing/historical_events folder and extracts all event IDs
and their commence times, then saves them to storage in the
odds/transformation/historical_event_id folder.
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


class EventDataExtractor:
    """
    Class to extract event IDs and commence times from historical events data stored in GCS.
    """

    def __init__(self):
        """Initialize the extractor with SmartbettingLib for GCS operations."""
        self.smartbetting = SmartbettingLib()
        self.bucket = Bucket.LAMJAV_STORAGE
        self.catalog = Catalog.ODDS
        self.schema = Schema.LANDING
        self.table = Table.HISTORICAL_EVENTS
        # Output path for saving event data
        self.output_catalog = Catalog.ODDS
        self.output_schema = "etl"
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

            print(f"Found {len(file_names)} files")
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

            return events_data

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            return []

    def extract_event_data(
        self, start_date: date = None, end_date: date = None
    ) -> Dict[str, str]:
        """
        Extract all event IDs and commence times from historical events files.

        Args:
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (inclusive)

        Returns:
            Dictionary mapping event ID to commence time
        """
        print("Extracting event data...")

        # List all files
        file_names = self.list_historical_events_files()
        if not file_names:
            print("No historical events files found")
            return {}

        # Filter files by date if specified
        if start_date or end_date:
            filtered_files = []
            for file_name in file_names:
                # Extract date from filename: historical_events_YYYY-MM-DD.json
                try:
                    date_str = file_name.split("_")[-1].replace(".json", "")
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").date()

                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue

                    filtered_files.append(file_name)
                except ValueError:
                    continue

            file_names = filtered_files

        # Extract event data from all files
        event_data = {}
        total_events = 0

        for file_name in file_names:
            events_data = self.read_historical_events_file(file_name)

            for event in events_data:
                event_id = event.get("id")
                commence_time = event.get("commence_time")
                if event_id and commence_time:
                    event_data[event_id] = commence_time
                    total_events += 1

        print(f"Found {len(event_data)} unique events")
        return event_data

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

    def save_event_data_to_storage(
        self, event_data: Dict[str, str], target_date: date = None
    ) -> str:
        """
        Save extracted event data to Google Cloud Storage.

        Args:
            event_data: Dictionary mapping event ID to commence time
            target_date: Date for the filename (defaults to today)

        Returns:
            The GCS blob name where the file was saved
        """
        if target_date is None:
            target_date = date.today()

        # Create filename with date: historical_event_id_YYYY-MM-DD.json
        filename = f"historical_event_id_{target_date.strftime('%Y-%m-%d')}.json"

        # Create the full GCS path
        gcs_path = (
            f"{self.output_catalog}/{self.output_schema}/{self.output_table}/{filename}"
        )

        # Prepare data structure
        data = {
            "extraction_date": target_date.isoformat(),
            "total_events": len(event_data),
            "events": [
                {"id": event_id, "commence_time": commence_time}
                for event_id, commence_time in sorted(event_data.items())
            ],
            "metadata": {
                "source": "historical_events",
                "extraction_timestamp": datetime.now().isoformat(),
                "file_format": "json",
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

            print(f"✅ Saved {len(event_data)} events to GCS")
            return gcs_path

        except Exception as e:
            print(f"❌ Error saving event data to storage: {e}")
            return None

    def extract_and_save_event_data(
        self, start_date: date = None, end_date: date = None, target_date: date = None
    ) -> Dict[str, str]:
        """
        Extract event data and save them to storage in one operation.

        Args:
            start_date: Optional start date filter for extraction (inclusive)
            end_date: Optional end date filter for extraction (inclusive)
            target_date: Date for the output filename (defaults to today)

        Returns:
            Dictionary of extracted event data (ID -> commence_time)
        """
        # Extract event data
        event_data = self.extract_event_data(start_date, end_date)

        if event_data:
            # Save to storage
            saved_path = self.save_event_data_to_storage(event_data, target_date)
            if not saved_path:
                print("⚠️  Failed to save event data to storage")
        else:
            print("⚠️  No event data to save")

        return event_data


def main():
    """
    Main function to extract and save event data.
    """
    extractor = EventDataExtractor()

    print("Extracting event IDs and commence times...")
    event_data = extractor.extract_and_save_event_data()

    if event_data:
        print(f"✅ Successfully processed {len(event_data)} events")

        # Show just a few sample events
        sample_items = list(event_data.items())[:3]
        print(f"\nSample events:")
        for i, (event_id, commence_time) in enumerate(sample_items, 1):
            print(f"{i}. ID: {event_id[:20]}... | Commence: {commence_time}")
    else:
        print("❌ No event data found")


if __name__ == "__main__":
    main()
