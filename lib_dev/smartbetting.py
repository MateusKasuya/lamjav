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
            objects: List of objects that have a model_dump() method or are already dicts

        Returns:
            List of dictionaries converted from the objects

        Raises:
            AttributeError: If objects don't have model_dump() method and aren't dicts
        """
        print("Converting object to dict...")
        result = []
        for data in objects:
            if isinstance(data, dict):
                result.append(data)
            elif hasattr(data, "model_dump"):
                result.append(data.model_dump())
            else:
                # Try to convert to dict if it's a simple object
                result.append(dict(data))
        return result

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
    ) -> bool:
        """
        Upload PDF data to Google Cloud Storage bucket.

        Args:
            pdf_data: PDF data as bytes to upload
            bucket_name: Name of the GCS bucket (can be enum or string)
            blob_name: GCS blob name/path (can be enum or string)

        Returns:
            bool: True if successful, False otherwise

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an issue with GCP credentials
            google.cloud.exceptions.NotFound: If the bucket doesn't exist
        """
        try:
            print("Uploading PDF to Google Cloud Storage...")
            storage_client = storage.Client()
            bucket = storage_client.bucket(str(bucket_name))
            blob = bucket.blob(str(blob_name))

            blob.upload_from_string(pdf_data, content_type="application/pdf")
            print(f"PDF uploaded to Google Cloud Storage!!! Size: {len(pdf_data)} bytes")
            return True
        except Exception as e:
            print(f"❌ Erro no upload para GCS: {str(e)}")
            return False

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

        # Se for WRITE_APPEND e temos source_file, deletar dados antigos da mesma data
        if write_disposition == "WRITE_APPEND" and "source_file" in df.columns:
            self._delete_old_data_by_date(client, project_id, dataset_id, table_id, df)

        # Configure job with explicit schema
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            create_disposition="CREATE_IF_NEEDED",
            schema=[
                bigquery.SchemaField("player_name", "STRING"),
                bigquery.SchemaField("current_status", "STRING"),
                bigquery.SchemaField("source_file", "STRING"),
                bigquery.SchemaField("row_order", "INTEGER"),
                bigquery.SchemaField("extraction_timestamp", "STRING"),
            ],
        )

        try:
            # Upload DataFrame to BigQuery
            job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)

            # Wait for completion
            job.result()

        except Exception:
            raise

    def _delete_old_data_by_date(
        self, client, project_id: str, dataset_id: str, table_id: str, df: pd.DataFrame
    ) -> None:
        """
        Deleta dados antigos da mesma data antes de inserir novos dados.

        Args:
            client: BigQuery client
            project_id: GCP project ID
            dataset_id: BigQuery dataset name
            table_id: BigQuery table name
            df: DataFrame com os novos dados
        """
        try:
            # Extrair datas únicas dos arquivos processados
            unique_dates = set()
            for source_file in df["source_file"].unique():
                # Extrair data do nome do arquivo (formato: injury_report_YYYY-MM-DD_06PM.pdf)
                if "injury_report_" in source_file and "_06PM.pdf" in source_file:
                    try:
                        date_part = source_file.split("injury_report_")[1].split(
                            "_06PM.pdf"
                        )[0]
                        unique_dates.add(date_part)
                    except:
                        continue

            # Deletar dados antigos das mesmas datas
            if unique_dates:
                print(f"🗑️ Deletando dados antigos das datas: {list(unique_dates)}")
                for date in unique_dates:
                    delete_query = f"""
                    DELETE FROM `{project_id}.{dataset_id}.{table_id}`
                    WHERE source_file LIKE '%injury_report_{date}_06PM.pdf%'
                    """
                    try:
                        query_job = client.query(delete_query)
                        query_job.result()  # Aguardar conclusão
                        print(f"✅ Dados antigos da data {date} deletados")
                    except Exception as e:
                        print(
                            f"⚠️ Aviso: Erro ao deletar dados antigos da data {date}: {e}"
                        )
        except Exception as e:
            print(f"⚠️ Aviso: Erro na limpeza de dados antigos: {e}")

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

    # ========================================================================================
    # CURRENT EVENTS DATA HELPERS (non-historical)
    # ========================================================================================

    def build_events_gcs_path(
        self,
        catalog: str,
        table: str,
        season: str,
        file_date: date,
    ) -> str:
        """
        Build a standard GCS path for events data files.

        Example path: odds/events/season_2024/raw_odds_events_YYYY-MM-DD.json
        """
        file_name = f"raw_{catalog}_{table}_{file_date.strftime('%Y-%m-%d')}.json"
        return f"{catalog}/{table}/{season}/{file_name}"

    def list_events_files(
        self, bucket_name: str, catalog: str, table: str, season: str
    ) -> List[str]:
        """
        List all current events files in the GCS folder.
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            prefix = f"{catalog}/{table}/{season}/"
            blobs = bucket.list_blobs(prefix=prefix)
            file_names: List[str] = []
            for blob in blobs:
                if blob.name.endswith(".json"):
                    file_names.append(blob.name)
            return file_names
        except Exception as e:
            print(f"Error listing events files: {e}")
            return []

    def read_events_file(self, bucket_name: str, file_name: str) -> List[Dict[str, Any]]:
        """
        Read a single events file (NDJSON) from GCS and parse it.
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_name)
            content = blob.download_as_text()
            events_data: List[Dict[str, Any]] = []
            for line in content.strip().split("\n"):
                if line.strip():
                    try:
                        events_data.append(json.loads(line))
                    except json.JSONDecodeError as json_err:
                        print(f"Error parsing JSON line in {file_name}: {json_err}")
                        continue
            return events_data
        except Exception as e:
            print(f"Error reading events file {file_name}: {e}")
            return []

    def extract_event_ids_from_events_data(
        self,
        bucket_name: str,
        catalog: str = "odds",
        table: str = "events",
        season: str = "season_2024",
        start_date: date = None,
        end_date: date = None,
    ) -> Dict[str, str]:
        """
        Extract event IDs and commence times from current events files in GCS.
        """
        print("🚀 Extracting event IDs from current events data...")
        file_names = self.list_events_files(bucket_name, catalog, table, season)
        if not file_names:
            print("No events files found")
            return {}

        # Filter files by date if specified
        if start_date or end_date:
            filtered_files: List[str] = []
            for file_name in file_names:
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

        event_data: Dict[str, str] = {}
        total_events = 0
        for file_name in file_names:
            events = self.read_events_file(bucket_name, file_name)
            for event in events:
                event_id = event.get("id")
                commence_time = event.get("commence_time")
                if event_id and commence_time:
                    event_data[event_id] = commence_time
                    total_events += 1
        print(
            f"✅ Extracted {len(event_data)} unique event IDs from {total_events} total current events"
        )
        return event_data

    def save_event_ids_to_storage(
        self,
        event_data: Dict[str, str],
        bucket_name: str,
        catalog: str,
        table: str,
        season: str,
        target_date: date = None,
    ) -> Optional[str]:
        """
        Save event IDs and commence times to GCS in a structured format.
        
        Args:
            event_data: Dictionary mapping event ID to commence time
            bucket_name: GCS bucket name
            catalog: Data catalog (e.g., 'odds')
            table: Table name (e.g., 'event_id')
            season: Season identifier (e.g., 'season_2025')
            target_date: Date for the extraction (defaults to today)
            
        Returns:
            GCS path if successful, None if failed
        """
        if target_date is None:
            target_date = date.today()

        filename = f"raw_{catalog}_{table}_{target_date.strftime('%Y-%m-%d')}.json"
        gcs_path = f"{catalog}/{table}/{season}/{filename}"

        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)

            data = {
                "extraction_date": target_date.isoformat(),
                "total_events": len(event_data),
                "events": [
                    {"id": event_id, "commence_time": commence_time}
                    for event_id, commence_time in sorted(event_data.items())
                ],
                "metadata": {
                    "source": "events",
                    "extraction_timestamp": datetime.now().isoformat(),
                    "file_format": "json",
                },
            }

            blob.upload_from_string(
                json.dumps(data, indent=2, ensure_ascii=False),
                content_type="application/json",
            )

            print(f"✅ Saved {len(event_data)} events to GCS")
            return gcs_path
        except Exception as e:
            print(f"❌ Error saving event data to storage: {e}")
            return None

    # ========================================================================================
    # INJURY REPORT PDF PROCESSING METHODS
    # ========================================================================================

    def list_pdf_files_in_gcs(
        self, bucket_name: str, prefix: str = "", target_dates: List[str] = None
    ) -> List[str]:
        """
        Lista arquivos PDF no Google Cloud Storage, filtrando por datas específicas.

        Args:
            bucket_name: Nome do bucket do GCS
            prefix: Prefixo para filtrar arquivos (opcional)
            target_dates: Lista de datas no formato YYYY-MM-DD para filtrar arquivos (opcional)

        Returns:
            Lista de nomes dos arquivos PDF filtrados por datas
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=prefix)
        all_pdf_files = [blob.name for blob in blobs if blob.name.lower().endswith(".pdf")]

        if target_dates:
            # Filtrar por múltiplas datas
            date_filtered_files = []
            for target_date in target_dates:
                files_for_date = [f for f in all_pdf_files if target_date in f]
                date_filtered_files.extend(files_for_date)
                print(f"🎯 PDFs da data {target_date}: {len(files_for_date)}")

            print(f"📁 Total PDFs no bucket: {len(all_pdf_files)}")
            print(f"🎯 Total PDFs das datas {target_dates}: {len(date_filtered_files)}")
        else:
            # Se não especificou datas, pegar todos os PDFs
            date_filtered_files = all_pdf_files
            print(f"📁 Total PDFs no bucket: {len(all_pdf_files)}")
            print("🎯 Processando todos os PDFs disponíveis")

        return date_filtered_files

    def download_pdf_from_gcs(self, bucket_name: str, blob_name: str, local_path: str) -> bool:
        """
        Baixa um arquivo PDF do GCS para um arquivo local temporário.

        Args:
            bucket_name: Nome do bucket
            blob_name: Nome do arquivo no GCS
            local_path: Caminho local para salvar

        Returns:
            True se sucesso, False caso contrário
        """
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            blob.download_to_filename(local_path)
            return True

        except Exception:
            return False

    def process_injury_report_pdfs(
        self,
        bucket_name: str,
        catalog: str,
        table: str,
        season: str,
        project_id: str,
        target_dates: List[str] = None,
    ) -> bool:
        """
        Processa PDFs de injury reports e insere no BigQuery.

        Args:
            bucket_name: Nome do bucket do GCS
            catalog: Catálogo de dados
            table: Nome da tabela
            season: Temporada para organização
            project_id: ID do projeto GCP
            target_dates: Lista de datas para filtrar

        Returns:
            bool: True se sucesso, False caso contrário
        """
        try:
            import tempfile
            import os
            import gc
            from lib_dev.pdfextractor import PDFTableExtractor

            # Configurar prefixo e paths
            pdf_prefix = f"{catalog}/{table}/{season}/"
            dataset_id = catalog
            table_id = f"raw_{table}"

            print(f"🚀 Pipeline Injury Report | Bucket: {bucket_name}")
            if target_dates:
                print(f"🎯 Filtrando por datas: {target_dates}")
            else:
                print("🎯 Processando todos os PDFs disponíveis")

            # 1. Listar arquivos PDF no GCS
            all_pdf_files = self.list_pdf_files_in_gcs(bucket_name, pdf_prefix, target_dates)

            if not all_pdf_files:
                print("✅ Nenhum arquivo encontrado para processar")
                return False

            print(f"📁 Total de PDFs encontrados: {len(all_pdf_files)}")

            # 2. Processar todos os PDFs: extrair dados de cada PDF
            all_dataframes = []
            error_stats = {
                "download_errors": 0,
                "extraction_errors": 0,
                "bigquery_errors": 0,
            }
            failed_files = []

            print(f"📥 Extraindo dados dos {len(all_pdf_files)} PDFs...")

            for i, pdf_file in enumerate(all_pdf_files, 1):
                filename = pdf_file.split("/")[-1]
                print(f"[{i}/{len(all_pdf_files)}] {filename}", end="")

                # Criar arquivo temporário
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                    temp_path = temp_file.name

                try:
                    # Baixar do GCS
                    if not self.download_pdf_from_gcs(bucket_name, pdf_file, temp_path):
                        print(" ❌ Download")
                        error_stats["download_errors"] += 1
                        failed_files.append(filename)
                        continue

                    # Extrair dados PDF
                    try:
                        extractor = PDFTableExtractor(temp_path)
                        df = extractor.get_all_players_from_pdf()

                        if df.empty:
                            print(" ⚠️ 0 linhas extraídas")
                        else:
                            df = extractor.sanitize_column_names(df)

                            # Verificar se coluna current_status está presente
                            if "current_status" in df.columns:
                                status_found = df["current_status"].value_counts()
                                print(f" 🔍 Status detectados: {len(status_found)} tipos diferentes")
                            else:
                                print(" ⚠️ Coluna 'current_status' não encontrada!")

                            # Adicionar metadados do arquivo fonte
                            df["source_file"] = pdf_file
                            df["row_order"] = range(1, len(df) + 1)

                            # Acumular DataFrame
                            all_dataframes.append(df)
                            print(f" ✅ {len(df)} linhas extraídas")

                        # Limpeza explícita de memória
                        del df
                        del extractor

                    except Exception as e:
                        error_msg = str(e)
                        print(f" ❌ Extração: {error_msg[:50]}...")
                        error_stats["extraction_errors"] += 1
                        failed_files.append(filename)

                finally:
                    # Limpar arquivo temporário
                    if os.path.exists(temp_path):
                        os.unlink(temp_path)

                    # Limpeza adicional de memória a cada 5 arquivos
                    if i % 5 == 0:
                        gc.collect()

            # 3. Consolidar dados e inserir no BigQuery
            total_rows = 0
            total_processed = 0

            if all_dataframes:
                print(f"\n📤 Consolidando e inserindo {len(all_dataframes)} PDFs no BigQuery...")

                try:
                    # Consolidar todos os DataFrames
                    combined_df = pd.concat(all_dataframes, ignore_index=True)

                    # Recalcular row_order global
                    combined_df["row_order"] = range(1, len(combined_df) + 1)
                    total_rows = len(combined_df)

                    # Inserir tudo no BigQuery de uma vez
                    self.upload_to_bigquery(
                        data=combined_df,
                        project_id=project_id,
                        dataset_id=dataset_id,
                        table_id=table_id,
                        write_disposition="WRITE_APPEND",
                    )

                    total_processed = len(all_dataframes)
                    print(f"✅ BigQuery: {total_processed} PDFs → {total_rows} registros inseridos")

                    # Limpeza total da memória
                    del combined_df
                    del all_dataframes
                    gc.collect()

                except Exception as e:
                    print(f"❌ Erro BigQuery: {str(e)}")
                    error_stats["bigquery_errors"] = 1

            # 4. Resumo final
            total_errors = sum(error_stats.values())
            success_rate = (total_processed / len(all_pdf_files)) * 100 if all_pdf_files else 0

            print("\n📊 RESUMO FINAL")
            print("=" * 50)
            print(f"📁 PDFs processados: {len(all_pdf_files)}")
            print(f"✅ Sucessos: {total_processed} ({success_rate:.1f}%)")
            print(f"❌ Falhas: {total_errors}")
            print(f"📊 Total de registros: {total_rows}")

            if total_processed > 0:
                print("💾 Dados inseridos no BigQuery com sucesso!")

            return total_processed > 0

        except Exception as e:
            print(f"❌ Erro no processamento: {e}")
            return False