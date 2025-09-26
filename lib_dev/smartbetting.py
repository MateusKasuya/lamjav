"""
Smartbetting utility library.

This module provides utilities for data conversion and GCS operations
for the Smartbetting project.
"""

from google.cloud import storage
from google.cloud import bigquery
import json
import pandas as pd
from typing import Any, List, Union, Optional, Dict
from datetime import datetime, date


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

    # ========================================================================================
    # EVENT DATA EXTRACTION METHODS
    # ========================================================================================

    def list_historical_events_files(
        self, bucket_name: str, catalog: str, table: str, season: str
    ) -> List[str]:
        """
        List all historical events files in the GCS folder.

        Args:
            bucket_name: GCS bucket name
            catalog: Data catalog (e.g., 'odds')
            table: Table name (e.g., 'historical_events')
            season: Season identifier (e.g., 'season_2024')

        Returns:
            List of file names (blob names) in the historical_events folder
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)

            # List all blobs in the historical_events folder
            prefix = f"{catalog}/{table}/{season}/"
            blobs = bucket.list_blobs(prefix=prefix)

            file_names = []
            for blob in blobs:
                if blob.name.endswith(".json"):
                    file_names.append(blob.name)

            print(f"Found {len(file_names)} historical events files")
            return file_names

        except Exception as e:
            print(f"Error listing historical events files: {e}")
            return []

    def read_historical_events_file(
        self, bucket_name: str, file_name: str
    ) -> List[Dict[str, Any]]:
        """
        Read a single historical events file from GCS.

        Note: Files are stored in NDJSON format (one JSON object per line).

        Args:
            bucket_name: GCS bucket name
            file_name: The GCS blob name to read

        Returns:
            List of event dictionaries from the file
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)

            # Download content as text
            content = blob.download_as_text()

            # Parse NDJSON format (one JSON object per line)
            events_data = []
            for line in content.strip().split("\n"):
                if line.strip():  # Skip empty lines
                    try:
                        event = json.loads(line)
                        events_data.append(event)
                    except json.JSONDecodeError as json_err:
                        print(f"Error parsing JSON line in {file_name}: {json_err}")
                        continue

            print(f"Successfully read {len(events_data)} events from {file_name}")
            return events_data

        except Exception as e:
            print(f"Error reading file {file_name}: {e}")
            return []

    def extract_event_ids_from_historical_data(
        self,
        bucket_name: str,
        catalog: str = "odds",
        table: str = "historical_events",
        season: str = "season_2024",
        start_date: date = None,
        end_date: date = None,
    ) -> Dict[str, str]:
        """
        Extract all event IDs and commence times from historical events files.

        This method provides the SAME functionality as extract_event_ids.py
        but integrated directly into SmartbettingLib for seamless usage.

        Args:
            bucket_name: GCS bucket name
            catalog: Data catalog (default: 'odds')
            table: Table name (default: 'historical_events')
            season: Season identifier (default: 'season_2024')
            start_date: Optional start date filter (inclusive)
            end_date: Optional end date filter (inclusive)

        Returns:
            Dictionary mapping event ID to commence time
        """
        print("🚀 Extracting event IDs from historical data...")

        # List all files
        file_names = self.list_historical_events_files(
            bucket_name, catalog, table, season
        )
        if not file_names:
            print("No historical events files found")
            return {}

        # Filter files by date if specified
        if start_date or end_date:
            filtered_files = []
            for file_name in file_names:
                # Extract date from filename: raw_odds_historical_events_YYYY-MM-DD.json
                try:
                    # Split filename and get the date part
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
            print(f"Filtered to {len(file_names)} files based on date range")

        # Extract event data from all files
        event_data = {}
        total_events = 0

        for file_name in file_names:
            events_data = self.read_historical_events_file(bucket_name, file_name)

            for event in events_data:
                event_id = event.get("id")
                commence_time = event.get("commence_time")
                if event_id and commence_time:
                    event_data[event_id] = commence_time
                    total_events += 1

        print(
            f"✅ Extracted {len(event_data)} unique event IDs from {total_events} total events"
        )
        return event_data
