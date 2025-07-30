"""
Smartbetting utility library.

This module provides utilities for data conversion and GCS operations
for the Smartbetting project.
"""

from google.cloud import storage
import json
from typing import Any, List, Union


class SmartbettingLib:
    """
    Utility class for Smartbetting data operations.

    This class provides methods for converting data formats and
    uploading data to Google Cloud Storage.
    """

    def convert_to_json(self, data: Union[List[dict], dict]) -> str:
        """
        Convert data to JSON format.

        Args:
            data: Data to be converted to JSON. Can be a list of dictionaries
                  or a single dictionary.

        Returns:
            JSON string representation of the data

        Raises:
            TypeError: If the data cannot be serialized to JSON
        """
        print("Converting to JSON...")
        return json.dumps(data, indent=2)

    def convert_to_ndjson(self, data: Union[List[dict], dict]) -> str:
        """
        Convert data to NEWLINE DELIMITED JSON format for BigQuery external tables.

        Args:
            data: Data to be converted to NDJSON. Can be a list of dictionaries
                  or a single dictionary.

        Returns:
            NDJSON string representation of the data (one JSON object per line)

        Raises:
            TypeError: If the data cannot be serialized to JSON
        """
        print("Converting to NDJSON...")
        if isinstance(data, list):
            return "\n".join(json.dumps(item) for item in data)
        else:
            return json.dumps(data)

    def convert_object_to_dict(self, objects: List[Any]) -> List[dict]:
        """
        Convert a list of objects to a list of dictionaries.

        Args:
            objects: List of objects that have a model_dump() method

        Returns:
            List of dictionaries converted from the objects

        Raises:
            AttributeError: If objects don't have model_dump() method
        """
        print("Converting object to dict...")
        return [data.model_dump() for data in objects]

    def upload_json_to_gcs(
        self, json_data: str, bucket_name: Union[str, Any], blob_name: Union[str, Any]
    ) -> None:
        """
        Upload JSON data to Google Cloud Storage bucket.

        Args:
            json_data: JSON string to upload
            bucket_name: Name of the GCS bucket (can be enum or string)
            blob_name: GCS blob name/path (can be enum or string)

        Returns:
            None

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an issue with GCP credentials
            google.cloud.exceptions.NotFound: If the bucket doesn't exist
        """
        print("Uploading JSON to Google Cloud Storage...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(str(bucket_name))
        blob = bucket.blob(str(blob_name))

        blob.upload_from_string(json_data, content_type="application/json")
        print("JSON uploaded to Google Cloud Storage!!!")
