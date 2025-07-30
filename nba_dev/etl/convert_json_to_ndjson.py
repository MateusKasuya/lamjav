"""
Convert JSON array files to NDJSON format for BigQuery compatibility.

This script downloads JSON array files from GCS, converts them to
NEWLINE_DELIMITED_JSON format, and uploads them back to GCS.
"""

import sys
import os
import json
from google.cloud import storage
from typing import List, Dict, Any

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.utils import Bucket


class JsonToNdjsonConverter:
    """Convert JSON array files to NDJSON format for BigQuery."""

    def __init__(self):
        """Initialize the converter with GCS client."""
        self.storage_client = storage.Client()
        self.bucket_name = str(Bucket.LAMJAV_STORAGE)

    def download_json_from_gcs(self, blob_path: str) -> List[Dict[Any, Any]]:
        """
        Download JSON file from GCS and parse as array.

        Args:
            blob_path: Path to the blob in GCS

        Returns:
            List of dictionaries from JSON array
        """
        print(f"Downloading {blob_path} from GCS...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_path)

        json_content = blob.download_as_text()
        data = json.loads(json_content)

        print(f"Downloaded {len(data)} records")
        return data

    def convert_to_ndjson(self, data: List[Dict[Any, Any]]) -> str:
        """
        Convert list of dictionaries to NDJSON format.

        Args:
            data: List of dictionaries

        Returns:
            NDJSON string (one JSON object per line)
        """
        print("Converting to NDJSON format...")
        return "\n".join(json.dumps(item) for item in data)

    def upload_ndjson_to_gcs(self, ndjson_data: str, blob_path: str) -> None:
        """
        Upload NDJSON data to GCS.

        Args:
            ndjson_data: NDJSON string
            blob_path: Destination path in GCS
        """
        print(f"Uploading NDJSON to {blob_path}...")
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(ndjson_data, content_type="application/json")
        print("Successfully uploaded NDJSON to GCS")

    def convert_file(self, source_path: str, destination_path: str) -> None:
        """
        Convert a single JSON array file to NDJSON.

        Args:
            source_path: Source file path in GCS
            destination_path: Destination file path in GCS
        """
        try:
            # Download original JSON array
            data = self.download_json_from_gcs(source_path)

            # Convert to NDJSON
            ndjson_data = self.convert_to_ndjson(data)

            # Upload converted file
            self.upload_ndjson_to_gcs(ndjson_data, destination_path)

            print(f"✅ Successfully converted {source_path} to {destination_path}")

        except Exception as e:
            print(f"❌ Error converting {source_path}: {str(e)}")
            raise


def convert_active_players():
    """Convert active_players JSON files to NDJSON format."""
    converter = JsonToNdjsonConverter()

    # Example file paths - adjust dates as needed
    source_file = "nba/landing/active_players/active_players_2025-07-12.json"
    destination_file = "nba/landing/active_players/active_players_2025-07-12.json"

    print("🚀 Starting active_players conversion...")
    converter.convert_file(source_file, destination_file)
    print("🎉 Active players conversion completed!")


if __name__ == "__main__":
    convert_active_players()
