"""
Convert all existing JSON array files to NDJSON format.

This script scans GCS for existing JSON files, converts them from
array format to NEWLINE_DELIMITED_JSON, and replaces the original files.
"""

import sys
import os
from google.cloud import storage

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from nba_dev.etl.convert_json_to_ndjson import JsonToNdjsonConverter
from lib_dev.utils import Bucket


def list_json_files_in_gcs(bucket_name: str, prefix: str = "nba/landing/") -> list:
    """
    List all JSON files in GCS bucket with given prefix.

    Args:
        bucket_name: Name of the GCS bucket
        prefix: Prefix to filter files

    Returns:
        List of blob names (file paths)
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    json_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    return json_files


def convert_all_active_players():
    """Convert all active_players JSON files to NDJSON."""
    converter = JsonToNdjsonConverter()
    bucket_name = str(Bucket.LAMJAV_STORAGE)

    # List all active_players files
    active_players_files = list_json_files_in_gcs(
        bucket_name, "nba/landing/game_player_stats/"
    )

    print(f"Found {len(active_players_files)} active_players files to convert:")
    for file_path in active_players_files:
        print(f"  - {file_path}")

    # Convert each file
    for file_path in active_players_files:
        print(f"\n🚀 Converting {file_path}...")
        try:
            converter.convert_file(file_path, file_path)  # Replace original
            print(f"✅ Successfully converted {file_path}")
        except Exception as e:
            print(f"❌ Failed to convert {file_path}: {str(e)}")

    print(f"\n🎉 Conversion completed! Processed {len(active_players_files)} files.")


def convert_all_teams():
    """Convert all teams JSON files to NDJSON."""
    converter = JsonToNdjsonConverter()
    bucket_name = str(Bucket.LAMJAV_STORAGE)

    # List all teams files
    teams_files = list_json_files_in_gcs(bucket_name, "nba/landing/teams/")

    print(f"Found {len(teams_files)} teams files to convert:")
    for file_path in teams_files:
        print(f"  - {file_path}")

    # Convert each file
    for file_path in teams_files:
        print(f"\n🚀 Converting {file_path}...")
        try:
            converter.convert_file(file_path, file_path)  # Replace original
            print(f"✅ Successfully converted {file_path}")
        except Exception as e:
            print(f"❌ Failed to convert {file_path}: {str(e)}")

    print(f"\n🎉 Conversion completed! Processed {len(teams_files)} files.")


def convert_all_nba_files():
    """Convert all NBA JSON files to NDJSON."""
    print("🔄 Starting conversion of all NBA JSON files to NDJSON format...\n")

    # Convert teams first (smaller files)
    print("=" * 50)
    print("CONVERTING TEAMS")
    print("=" * 50)
    # convert_all_teams()

    # Convert active players
    print("\n" + "=" * 50)
    print("CONVERTING ACTIVE PLAYERS")
    print("=" * 50)
    convert_all_active_players()

    print("\n🎉 All conversions completed successfully!")
    print(
        "💡 Your files are now in NDJSON format and ready for BigQuery external tables!"
    )


if __name__ == "__main__":
    convert_all_nba_files()
