"""
Smartbetting utility library.

This module provides utilities for data conversion and GCS operations
for the Smartbetting project.
"""

from google.cloud import storage
from google.cloud import bigquery
import json
import pandas as pd
from typing import Any, List, Union, Optional
from datetime import datetime


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

    def upload_pdf_to_gcs(
        self, pdf_data: bytes, bucket_name: Union[str, Any], blob_name: Union[str, Any]
    ) -> None:
        """
        Upload PDF data to Google Cloud Storage bucket.

        Args:
            pdf_data: PDF data as bytes to upload
            bucket_name: Name of the GCS bucket (can be enum or string)
            blob_name: GCS blob name/path (can be enum or string)

        Returns:
            None

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an issue with GCP credentials
            google.cloud.exceptions.NotFound: If the bucket doesn't exist
        """
        print("Uploading PDF to Google Cloud Storage...")
        storage_client = storage.Client()
        bucket = storage_client.bucket(str(bucket_name))
        blob = bucket.blob(str(blob_name))

        blob.upload_from_string(pdf_data, content_type="application/pdf")
        print(f"PDF uploaded to Google Cloud Storage!!! Size: {len(pdf_data)} bytes")

    def upload_to_bigquery(
        self,
        data: Union[pd.DataFrame, List[dict], dict],
        project_id: str,
        dataset_id: str,
        table_id: str,
        write_disposition: str = "WRITE_APPEND",
        source_file: Optional[str] = None,
    ) -> None:
        """
        Upload data directly to BigQuery table.

        Args:
            data: Data to upload (DataFrame, list of dicts, or single dict)
            project_id: GCP project ID
            dataset_id: BigQuery dataset name
            table_id: BigQuery table name
            write_disposition: How to handle existing data (WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY)
            source_file: Optional source file name for metadata

        Returns:
            None

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an issue with BigQuery
        """
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)

        # Ensure dataset exists
        self._ensure_dataset_exists(client, dataset_id)

        # Convert data to DataFrame if needed
        if isinstance(data, dict):
            df = pd.DataFrame([data])
        elif isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            raise TypeError(f"Unsupported data type: {type(data)}")

        # Add metadata columns
        df["extraction_timestamp"] = datetime.now().isoformat()
        if source_file:
            df["source_file"] = source_file

        # Table reference
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Configure job
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            autodetect=True,  # Auto-detect schema
            create_disposition="CREATE_IF_NEEDED",
        )

        try:
            # Upload DataFrame to BigQuery
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

            # Wait for completion
            job.result()

        except Exception:
            raise

    def upload_to_gcs(
        self,
        bucket_name: str,
        data: str,
        catalog: str,
        schema: str,
        table: str,
        file_name: str,
    ) -> None:
        """
        Upload data to Google Cloud Storage with organized path structure.

        Args:
            bucket_name: GCS bucket name
            data: Data to upload (string format)
            catalog: Data catalog (e.g., 'nba', 'odds', 'pdf')
            schema: Data schema (e.g., 'landing', 'staging')
            table: Table name
            file_name: File name

        Returns:
            None

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an issue with GCS
        """
        print("Uploading data to Google Cloud Storage...")

        # Build path: catalog/schema/table/file_name
        blob_path = f"{catalog}/{schema}/{table}/{file_name}"

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        blob.upload_from_string(data, content_type="application/json")
        print(f"✅ Data uploaded to gs://{bucket_name}/{blob_path}")

    def _ensure_dataset_exists(self, client: bigquery.Client, dataset_id: str) -> None:
        """
        Ensure BigQuery dataset exists, create if not.

        Args:
            client: BigQuery client
            dataset_id: Dataset ID to check/create

        Returns:
            None
        """
        try:
            client.get_dataset(dataset_id)
        except Exception:
            dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
            dataset.location = "US"
            client.create_dataset(dataset, timeout=30)

    def print_summary(
        self,
        successful_extractions: int,
        failed_extractions: int,
        total_combinations: int,
        extraction_date: str,
        category_name: str = "Season Averages",
    ) -> None:
        """
        Print a summary of the extraction results.

        Args:
            successful_extractions: Number of successful extractions
            failed_extractions: Number of failed extractions
            total_combinations: Total number of combinations processed
            extraction_date: Date of extraction
            category_name: Name of the category being processed
        """
        print("\n" + "=" * 80)
        print(f"{category_name.upper()} EXTRACTION SUMMARY:")
        print(f"✅ Successful extractions: {successful_extractions}")
        print(f"❌ Failed extractions: {failed_extractions}")
        print(f"📊 Total combinations processed: {total_combinations}")
        print(f"📅 Extraction date: {extraction_date}")

        if successful_extractions > 0:
            print(
                f"\n🎉 Successfully extracted {successful_extractions} {category_name} season averages datasets!"
            )
        else:
            print(
                f"\n⚠️  No successful extractions for {category_name}. Check API configuration and parameters."
            )
