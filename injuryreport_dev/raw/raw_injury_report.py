"""
Raw Injury Report data pipeline script.

Este script processa arquivos PDF de injury reports do Google Cloud Storage em batches:
- Filtra especificamente o arquivo injury_report_2025-04-07_06PM.pdf
- Processa 50 PDFs por vez
- Consolida dados do batch completo
- Insere tudo no BigQuery de uma vez
- Limpa memória completamente
- Continua para o próximo batch
"""

import sys
import os
import tempfile
from typing import NoReturn, List
from dotenv import load_dotenv
from google.cloud import storage
import pandas as pd

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from lib_dev.pdfextractor import PDFTableExtractor
from lib_dev.utils import Bucket, Catalog, Schema, Table


# Lista de arquivos já processados (para continuar do último)
# Lista vazia pois vamos processar apenas 2025-04-07
PROCESSED_FILES = {}


def list_pdf_files_in_gcs(bucket_name: str, prefix: str = "") -> List[str]:
    """
    Lista arquivos PDF no Google Cloud Storage, filtrando os já processados.
    Agora filtra especificamente o arquivo injury_report_2025-04-07_06PM.pdf.

    Args:
        bucket_name: Nome do bucket do GCS
        prefix: Prefixo para filtrar arquivos (opcional)

    Returns:
        Lista de nomes dos arquivos PDF não processados (especificamente 2025-04-07_06PM)
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)
    all_pdf_files = [blob.name for blob in blobs if blob.name.lower().endswith(".pdf")]

    # Filtrar apenas o arquivo específico: injury_report_2025-04-07_06PM.pdf
    target_filename = "2025-04-07_06PM"
    date_filtered_files = [f for f in all_pdf_files if target_filename in f]

    # Filtrar apenas arquivos não processados
    pdf_files = [f for f in date_filtered_files if f not in PROCESSED_FILES]

    print(f"📁 Total PDFs no bucket: {len(all_pdf_files)}")
    print(
        f"🎯 PDFs do arquivo específico {target_filename}: {len(date_filtered_files)}"
    )
    print(f"🆕 PDFs novos para processar: {len(pdf_files)}")
    return pdf_files


def download_pdf_from_gcs(bucket_name: str, blob_name: str, local_path: str) -> bool:
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


def main(batch_size: int = 50, start_from: int = 0) -> NoReturn:
    """
    Pipeline principal para processar PDFs do GCS em batches e inserir no BigQuery.
    Agora filtra especificamente o arquivo injury_report_2025-04-07_06PM.pdf.

    Este processo:
    1. Lista arquivos PDF no Google Cloud Storage (especificamente 2025-04-07_06PM)
    2. Para cada batch de PDFs:
       - Baixa e extrai dados de cada PDF
       - Acumula dados do batch completo
       - Insere tudo no BigQuery de uma vez
       - Limpa memória completamente
    3. Continua para o próximo batch

    Args:
        batch_size: Número de PDFs a processar por batch (padrão: 50)
        start_from: Índice para continuar processamento (padrão: 0)

    Raises:
        ValueError: Se as variáveis de ambiente não estiverem configuradas
        Exception: Para outros erros inesperados
    """
    # Carrega variáveis de ambiente
    load_dotenv()

    # Configurações usando utils.py
    project_id = os.getenv("DBT_PROJECT")
    bucket_name = str(Bucket.SMARTBETTING_STORAGE)
    pdf_prefix = f"{str(Catalog.INJURY_REPORT)}/{str(Schema.LANDING)}/"

    dataset_id = str(Catalog.INJURY_REPORT)
    table_id = f"{str(Schema.RAW)}_{str(Table.INJURY_REPORT)}"

    # Validação de configuração
    if not project_id:
        raise ValueError("Variável de ambiente DBT_PROJECT não está configurada")

    print(f"🚀 Pipeline Injury Report | Bucket: {bucket_name}")
    print("🎯 Filtrando especificamente: injury_report_2025-04-07_06PM.pdf")
    print(f"⚙️ Processamento: Batch Size={batch_size}, Start From={start_from}")
    print(
        f"🎯 Fluxo: Extrair {batch_size} PDFs → BigQuery → Limpar Memória → Próximo Batch"
    )

    try:
        # 1. Listar arquivos PDF no GCS
        all_pdf_files = list_pdf_files_in_gcs(bucket_name, pdf_prefix)

        if not all_pdf_files:
            print("✅ Nenhum arquivo novo para processar")
            return

        # 2. Selecionar batch para processar
        total_files = len(all_pdf_files)
        end_index = min(start_from + batch_size, total_files)
        pdf_files = all_pdf_files[start_from:end_index]

        print(
            f"📁 Total: {total_files} PDFs | Processando: {len(pdf_files)} (índices {start_from}-{end_index - 1})"
        )

        if not pdf_files:
            print("✅ Nenhum arquivo no batch selecionado")
            return

        # 3. Processar batch: extrair dados de todos os PDFs
        batch_dataframes = []  # Acumular DataFrames do batch
        error_stats = {
            "download_errors": 0,
            "extraction_errors": 0,
            "bigquery_errors": 0,
        }
        failed_files = []

        print(f"📥 Extraindo dados dos {len(pdf_files)} PDFs do batch...")

        for i, pdf_file in enumerate(pdf_files, 1):
            filename = pdf_file.split("/")[-1]
            print(f"[{i}/{len(pdf_files)}] {filename}", end="")

            # Criar arquivo temporário
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_file:
                temp_path = temp_file.name

            try:
                # Baixar do GCS
                if not download_pdf_from_gcs(bucket_name, pdf_file, temp_path):
                    print(" ❌ Download")
                    error_stats["download_errors"] += 1
                    failed_files.append(filename)
                    continue

                # Extrair dados PDF e acumular no batch
                try:
                    extractor = PDFTableExtractor(temp_path)

                    # Usar o novo método que captura todos os jogadores de todas as páginas
                    df = extractor.get_all_players_from_pdf()

                    if df.empty:
                        print(" ⚠️ 0 linhas extraídas")
                    else:
                        df = extractor.sanitize_column_names(df)

                        # Adicionar metadados do arquivo fonte
                        df["source_file"] = pdf_file

                        # Adicionar coluna de ordem das linhas para preservar sequência original
                        df["row_order"] = range(1, len(df) + 1)

                        # Acumular DataFrame do batch
                        batch_dataframes.append(df)
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

                # Limpeza adicional de memória a cada 10 arquivos
                if i % 10 == 0:
                    import gc

                    gc.collect()

        # 4. Consolidar dados do batch e inserir no BigQuery
        total_rows = 0
        total_processed = 0

        if batch_dataframes:
            print(
                f"\n📤 Consolidando e inserindo {len(batch_dataframes)} PDFs no BigQuery..."
            )

            try:
                # Consolidar todos os DataFrames do batch
                combined_df = pd.concat(batch_dataframes, ignore_index=True)

                # Recalcular row_order global para todo o batch
                combined_df["row_order"] = range(1, len(combined_df) + 1)
                total_rows = len(combined_df)

                # Inserir tudo no BigQuery de uma vez
                from lib_dev.smartbetting import SmartbettingLib

                smartbetting = SmartbettingLib()

                smartbetting.upload_to_bigquery(
                    data=combined_df,
                    project_id=project_id,
                    dataset_id=dataset_id,
                    table_id=table_id,
                    write_disposition="WRITE_APPEND",
                )

                total_processed = len(batch_dataframes)
                print(
                    f"✅ BigQuery: {total_processed} PDFs → {total_rows} registros inseridos"
                )

                # Limpeza total da memória do batch
                del combined_df
                del batch_dataframes
                import gc

                gc.collect()

            except Exception as e:
                print(f"❌ Erro BigQuery COMPLETO: {str(e)}")
                print(
                    f"📊 DataFrame info: Shape={combined_df.shape}, Columns={list(combined_df.columns)}"
                )
                print(f"📊 DataFrame dtypes:\n{combined_df.dtypes}")
                if hasattr(e, "errors") and e.errors:
                    print(f"📋 Detalhes do erro: {e.errors}")
                error_stats["bigquery_errors"] = 1

        # 5. Resumo final detalhado
        total_errors = sum(error_stats.values())
        success_rate = (total_processed / len(pdf_files)) * 100 if pdf_files else 0

        print("\n📊 RESUMO DO BATCH")
        print("=" * 50)
        print(f"📁 Batch processado: {len(pdf_files)} de {total_files} PDFs totais")
        print(f"📍 Índices: {start_from} a {end_index - 1}")
        print(f"✅ Sucessos: {total_processed} ({success_rate:.1f}%)")
        print(f"❌ Falhas: {total_errors}")
        print(f"📊 Total de registros: {total_rows}")

        if total_processed > 0:
            print("💾 Dados inseridos no BigQuery com sucesso!")

        # Informações sobre próximo batch
        if end_index < total_files:
            remaining = total_files - end_index
            print("\n🔄 PRÓXIMO BATCH:")
            print(f"   Restam {remaining} PDFs para processar")
            print(f"   Execute: main(batch_size={batch_size}, start_from={end_index})")

        if total_errors > 0:
            print("\n🔍 TIPOS DE ERRO:")
            for error_type, count in error_stats.items():
                if count > 0:
                    percentage = (count / total_errors) * 100
                    error_name = error_type.replace("_", " ").title()
                    print(f"  • {error_name}: {count} ({percentage:.1f}%)")

        # Listar alguns arquivos que falharam para investigação
        if failed_files:
            print("\n📋 ARQUIVOS QUE FALHARAM (primeiros 10):")
            for filename in failed_files[:10]:
                print(f"  • {filename}")
            if len(failed_files) > 10:
                print(f"  ... e mais {len(failed_files) - 10} arquivos")

        # Recomendações baseadas nos tipos de erro
        if (
            error_stats["download_errors"] > len(pdf_files) * 0.1
        ):  # Mais de 10% de erros de download
            print("\n⚠️ RECOMENDAÇÃO: Muitos erros de download detectados.")
            print("   Verifique conectividade com GCS e permissões.")

        if (
            error_stats["extraction_errors"] > len(pdf_files) * 0.2
        ):  # Mais de 20% de erros de extração
            print("\n⚠️ RECOMENDAÇÃO: Muitos erros de extração detectados.")
            print(
                "   Considere verificar o formato dos PDFs recentes ou ajustar coordenadas do Camelot."
            )

    except Exception as e:
        print(f"❌ Erro: {e}")
        raise


def process_all_batches(batch_size: int = 50):
    """
    Processa especificamente o arquivo injury_report_2025-04-07_06PM.pdf.
    Cada batch é inserido no BigQuery e memória é limpa antes do próximo.

    Args:
        batch_size: Tamanho de cada batch (padrão: 50)
    """
    import time

    print("🔄 Iniciando processamento do arquivo injury_report_2025-04-07_06PM.pdf")
    start_from = 0
    batch_num = 1

    while True:
        print(f"\n{'=' * 60}")
        print(f"🔥 BATCH #{batch_num} - Iniciando do índice {start_from}")
        print(f"{'=' * 60}")

        try:
            # Executa o batch
            main(batch_size=batch_size, start_from=start_from)

            # Pausa entre batches para não sobrecarregar o sistema
            print("\n⏱️ Pausa de 30 segundos entre batches...")
            time.sleep(30)

            start_from += batch_size
            batch_num += 1

        except Exception as e:
            print(f"\n❌ Erro no batch #{batch_num}: {e}")
            print(f"💡 Continue manualmente com: main(start_from={start_from})")
            break


if __name__ == "__main__":
    # Executa apenas um batch por padrão
    main()

    # Para processar todos automaticamente, descomente a linha abaixo:
    # process_all_batches()
