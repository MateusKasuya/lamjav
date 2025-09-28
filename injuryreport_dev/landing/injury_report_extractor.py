"""
NBA Injury Report Extractor - Unified Fetcher and Uploader.

Este script consolida todas as funcionalidades de extração de injury reports:
- Current: Relatório mais atual disponível
- Specific: Relatório específico por data e hora
- Historical: Múltiplos relatórios em um range de datas
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional, Union

# Add lib_dev to path
sys.path.append(str(Path(__file__).parent.parent.parent / "lib_dev"))

from injuryreport import NBAInjuryReport, ValidationError
from smartbetting import SmartbettingLib
from utils import Bucket, Catalog, Schema


class InjuryReportExtractor:
    """
    Extrator unificado de injury reports da NBA.
    """

    def __init__(self):
        """Inicializa os clientes necessários."""
        self.injury_client = NBAInjuryReport()
        self.storage_client = SmartbettingLib()

    def fetch_current_report(self, upload_to_gcs: bool = True) -> bool:
        """
        Busca o relatório de injury atual disponível.

        Args:
            upload_to_gcs: Se deve fazer upload para GCS automaticamente

        Returns:
            bool: True se sucesso, False caso contrário
        """
        print("=== Buscando Current Injury Report ===")
        print(f"Iniciado em: {datetime.now()}")

        try:
            print("\n1. Buscando relatório atual...")
            result = self.injury_client.fetch_current_report()

            if result is None:
                print("❌ Nenhum relatório atual disponível")
                return False

            pdf_data, filename = result
            print(f"✅ Sucesso: {filename} ({len(pdf_data)} bytes)")

            if upload_to_gcs:
                success = self._upload_to_gcs(pdf_data, filename)
                if success:
                    print("✅ Upload para GCS concluído com sucesso!")
                return success
            else:
                print("ℹ️ Upload para GCS desabilitado")
                return True

        except Exception as e:
            print(f"❌ Erro durante processamento: {str(e)}")
            return False

    def fetch_specific_report(
        self,
        report_date: Union[str, date],
        hour: int,
        period: str,
        upload_to_gcs: bool = True,
    ) -> bool:
        """
        Busca um relatório específico por data e hora.

        Args:
            report_date: Data do relatório (YYYY-MM-DD string ou objeto date)
            hour: Hora do relatório (1-12)
            period: Período do relatório ("AM" ou "PM")
            upload_to_gcs: Se deve fazer upload para GCS automaticamente

        Returns:
            bool: True se sucesso, False caso contrário
        """
        print("=== Buscando Specific Injury Report ===")
        print(f"Iniciado em: {datetime.now()}")

        try:
            # Validar e converter data
            if isinstance(report_date, str):
                target_date = self._validate_date_string(report_date)
            else:
                target_date = report_date

            # Validar hora e período
            if not (1 <= hour <= 12):
                raise ValueError(f"Hora deve estar entre 1 e 12, recebido: {hour}")

            if period not in ["AM", "PM"]:
                raise ValueError(f"Período deve ser 'AM' ou 'PM', recebido: {period}")

            print(f"\nRelatório alvo: {target_date} às {hour:02d}{period}")

            print(
                f"\n1. Buscando relatório para {target_date} às {hour:02d}{period}..."
            )
            result = self.injury_client.fetch_specific_report(
                report_date=target_date, hour=hour, period=period
            )

            if result is None:
                print(
                    f"❌ Relatório não encontrado para {target_date} às {hour:02d}{period}"
                )
                return False

            pdf_data, filename = result
            print(f"✅ Sucesso: {filename} ({len(pdf_data)} bytes)")

            if upload_to_gcs:
                success = self._upload_to_gcs(pdf_data, filename)
                if success:
                    print("✅ Upload para GCS concluído com sucesso!")
                return success
            else:
                print("ℹ️ Upload para GCS desabilitado")
                return True

        except ValidationError as e:
            print(f"❌ Erro de validação: {str(e)}")
            return False
        except ValueError as e:
            print(f"❌ Erro de valor: {str(e)}")
            return False
        except Exception as e:
            print(f"❌ Erro durante processamento: {str(e)}")
            return False

    def fetch_historical_reports(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        hours: Optional[List[Tuple[int, str]]] = None,
        upload_to_gcs: bool = True,
    ) -> Tuple[int, int, int]:
        """
        Busca múltiplos relatórios históricos em um range de datas.

        Args:
            start_date: Data inicial (YYYY-MM-DD string ou objeto date)
            end_date: Data final (YYYY-MM-DD string ou objeto date)
            hours: Lista de tuplas (hora, período). Se None, tenta todas as horas.
            upload_to_gcs: Se deve fazer upload para GCS automaticamente

        Returns:
            Tuple[int, int, int]: (total_fetched, successful_uploads, failed_operations)
        """
        print("=== Buscando Historical Injury Reports ===")
        print(f"Iniciado em: {datetime.now()}")

        try:
            # Validar e converter datas
            if isinstance(start_date, str):
                start_date = self._validate_date_string(start_date)
            if isinstance(end_date, str):
                end_date = self._validate_date_string(end_date)

            # Definir horas se não especificadas
            if hours is None:
                hours = self._generate_all_hours()
                hours_description = "TODAS as horas (1AM-12AM, 1PM-12PM)"
            else:
                hours_description = f"{len(hours)} combinações específicas"

            print(f"\nRange de datas: {start_date} até {end_date}")
            print(f"Horas para tentar: {hours_description}")
            print(f"Total de combinações por dia: {len(hours)}")

            print(
                f"\n1. Buscando e {'fazendo upload' if upload_to_gcs else 'coletando'} relatórios históricos..."
            )
            print("Usando abordagem streaming: buscar → upload → limpar memória")

            total_fetched = 0
            successful_uploads = 0
            failed_operations = 0

            current_date = start_date

            while current_date <= end_date:
                print(f"\n📅 Processando data: {current_date}")
                date_fetched = 0
                date_uploaded = 0

                for hour, period in hours:
                    try:
                        # Buscar relatório único
                        result = self.injury_client.fetch_specific_report(
                            report_date=current_date, hour=hour, period=period
                        )

                        if result is None:
                            failed_operations += 1
                            continue

                        pdf_data, filename = result
                        total_fetched += 1
                        date_fetched += 1

                        if upload_to_gcs:
                            # Upload imediato para GCS
                            try:
                                success = self._upload_to_gcs(pdf_data, filename)
                                if success:
                                    successful_uploads += 1
                                    date_uploaded += 1
                                    print(
                                        f"  ✅ {hour:02d}{period}: Buscado & enviado ({len(pdf_data)} bytes)"
                                    )
                                else:
                                    failed_operations += 1
                                    print(
                                        f"  ❌ {hour:02d}{period}: Buscado mas upload falhou"
                                    )

                                # Liberar memória imediatamente
                                del pdf_data

                            except Exception as upload_error:
                                failed_operations += 1
                                print(
                                    f"  ❌ {hour:02d}{period}: Upload falhou - {str(upload_error)}"
                                )
                                continue
                        else:
                            successful_uploads += 1
                            date_uploaded += 1
                            print(
                                f"  ✅ {hour:02d}{period}: Buscado ({len(pdf_data)} bytes)"
                            )
                            del pdf_data

                    except Exception as fetch_error:
                        failed_operations += 1
                        print(
                            f"  ⚠️ {hour:02d}{period}: Busca falhou - {str(fetch_error)}"
                        )
                        continue

                print(
                    f"  📊 Resumo do dia: {date_fetched} buscados, {date_uploaded} {'enviados' if upload_to_gcs else 'processados'}"
                )
                current_date += timedelta(days=1)

            print("\n📊 Resumo Final:")
            print(f"   📥 Total buscado: {total_fetched}")
            print(
                f"   ✅ {'Uploads' if upload_to_gcs else 'Processamentos'} bem-sucedidos: {successful_uploads}"
            )
            print(f"   ❌ Operações falhadas: {failed_operations}")

            if total_fetched > 0:
                success_rate = (successful_uploads / total_fetched) * 100
                print(f"   📈 Taxa de sucesso: {success_rate:.1f}%")

            if successful_uploads > 0:
                print("✅ Processamento de relatórios históricos concluído!")
            else:
                print("❌ Nenhum relatório foi processado com sucesso")

            print(f"Finalizado em: {datetime.now()}")
            return total_fetched, successful_uploads, failed_operations

        except ValidationError as e:
            print(f"❌ Erro de validação: {str(e)}")
            return 0, 0, 1
        except ValueError as e:
            print(f"❌ Erro de valor: {str(e)}")
            return 0, 0, 1
        except Exception as e:
            print(f"❌ Erro durante processamento: {str(e)}")
            return 0, 0, 1

    def fetch_date_range(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        preferred_hours: List[Tuple[int, str]] = None,
        upload_to_gcs: bool = True,
    ) -> Tuple[int, int, int]:
        """
        Método simplificado para buscar relatórios em um range de datas com horas preferenciais.

        Args:
            start_date: Data inicial
            end_date: Data final
            preferred_hours: Lista de horas preferenciais. Padrão: [(6, "AM"), (6, "PM")]
            upload_to_gcs: Se deve fazer upload para GCS

        Returns:
            Tuple[int, int, int]: (total_fetched, successful_uploads, failed_operations)
        """
        if preferred_hours is None:
            preferred_hours = [(6, "AM"), (6, "PM")]

        return self.fetch_historical_reports(
            start_date=start_date,
            end_date=end_date,
            hours=preferred_hours,
            upload_to_gcs=upload_to_gcs,
        )

    def _upload_to_gcs(self, pdf_data: bytes, filename: str) -> bool:
        """
        Faz upload de um PDF para o Google Cloud Storage.

        Args:
            pdf_data: Dados binários do PDF
            filename: Nome do arquivo

        Returns:
            bool: True se sucesso, False caso contrário
        """
        try:
            gcs_blob_name = (
                f"{str(Catalog.INJURY_REPORT)}/{str(Schema.LANDING)}/{filename}"
            )

            self.storage_client.upload_pdf_to_gcs(
                pdf_data=pdf_data,
                bucket_name=Bucket.SMARTBETTING_STORAGE,
                blob_name=gcs_blob_name,
            )
            return True

        except Exception as e:
            print(f"❌ Erro no upload para GCS: {str(e)}")
            return False

    def _validate_date_string(self, date_string: str) -> date:
        """
        Valida e converte string de data.

        Args:
            date_string: String de data no formato YYYY-MM-DD

        Returns:
            date: Objeto date validado

        Raises:
            ValueError: Se formato de data for inválido
        """
        try:
            return datetime.strptime(date_string, "%Y-%m-%d").date()
        except ValueError as e:
            raise ValueError(
                f"Formato de data inválido. Use YYYY-MM-DD. Erro: {str(e)}"
            )

    def _generate_all_hours(self) -> List[Tuple[int, str]]:
        """
        Gera todas as horas possíveis (1AM-12AM, 1PM-12PM).

        Returns:
            Lista de todas as 24 combinações de horas como tuplas (hora, período)
        """
        all_hours = []

        # Horas AM: 12AM, 1AM, 2AM, ..., 11AM
        for hour in [12] + list(range(1, 12)):
            all_hours.append((hour, "AM"))

        # Horas PM: 12PM, 1PM, 2PM, ..., 11PM
        for hour in [12] + list(range(1, 12)):
            all_hours.append((hour, "PM"))

        return all_hours


def main():
    """
    Função principal - exemplo de uso do extrator.
    """
    print("🏀 NBA Injury Report Extractor - Versão Unificada")
    print(f"Iniciado em: {datetime.now()}")
    print("=" * 60)

    # Inicializar extrator
    extractor = InjuryReportExtractor()

    # Exemplo 1: Relatório atual
    print("\n📋 EXEMPLO 1: Relatório Atual")
    success_current = extractor.fetch_current_report()

    # Exemplo 2: Relatório específico
    print("\n📋 EXEMPLO 2: Relatório Específico")
    target_date = date.today() - timedelta(days=1)  # Ontem
    success_specific = extractor.fetch_specific_report(
        report_date=target_date, hour=6, period="PM"
    )

    # Exemplo 3: Relatórios históricos (últimos 3 dias)
    print("\n📋 EXEMPLO 3: Relatórios Históricos")
    end_date = "2025-04-20"  # Ontem
    start_date = "2025-04-11"  # 3 dias atrás

    total, uploaded, failed = extractor.fetch_date_range(
        start_date=start_date, end_date=end_date, preferred_hours=[(6, "PM")]
    )

    # Resumo final
    print("\n" + "=" * 60)
    print("📊 RESUMO GERAL:")
    print(f"  Relatório atual:     {'✅ Sucesso' if success_current else '❌ Falhou'}")
    print(
        f"  Relatório específico: {'✅ Sucesso' if success_specific else '❌ Falhou'}"
    )
    print(f"  Relatórios históricos: {uploaded} enviados de {total} buscados")
    print(f"Finalizado em: {datetime.now()}")

    return success_current or success_specific or (uploaded > 0)


if __name__ == "__main__":
    success = main()
    print(f"\n🎯 Status de saída: {'SUCESSO' if success else 'FALHA'}")
    sys.exit(0 if success else 1)
