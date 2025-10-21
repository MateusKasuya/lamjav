"""
NBA Injury Report client library.

This module provides functionality to fetch NBA injury reports from the official NBA website.
Supports fetching current reports, historical reports, and specific date/time reports.
"""

import requests
from typing import Optional, List, Tuple
from datetime import datetime, date, timedelta
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).parent.parent
load_dotenv(project_root / ".env")


class NBAInjuryReportException(Exception):
    """Base exception for NBA Injury Report errors."""

    def __init__(self, message: str, status_code: int = 0, response_data: dict = None):
        self.message = message
        self.status_code = status_code
        self.response_data = response_data or {}
        super().__init__(self.message)


class FetchError(NBAInjuryReportException):
    """Raised when a fetch operation fails."""

    pass


class ValidationError(NBAInjuryReportException):
    """Raised when input parameters are invalid."""

    pass


class NBAInjuryReport:
    """
    A client for fetching NBA injury reports from the official NBA website.

    This class provides methods to fetch current injury reports, historical reports
    within a date range, and specific reports by date and time. Returns PDF data
    as bytes for further processing.
    """

    def __init__(self, et_offset_hours: int = 1) -> None:
        """
        Initialize the NBA Injury Report client.

        Args:
            et_offset_hours: Hours to subtract from Brazil time to get ET.
                           1 for EDT (UTC-4), 2 for EST (UTC-5). Default: 1 (EDT)
        """
        self.base_url = "https://ak-static.cms.nba.com/referee/injury/"
        self.session = requests.Session()
        self.et_offset_hours = et_offset_hours

    def _handle_exceptions(self, e: Exception, operation: str) -> None:
        """
        Handle exceptions in a centralized way.

        Args:
            e: The exception that occurred
            operation: Description of the operation that failed
        """
        if isinstance(e, FetchError):
            print(f"Fetch error during {operation}: {e.message}")
        elif isinstance(e, ValidationError):
            print(f"Validation error during {operation}: {e.message}")
        elif isinstance(e, NBAInjuryReportException):
            print(f"General error during {operation}: {e.message}")
        elif isinstance(e, requests.RequestException):
            print(f"Network error during {operation}: {str(e)}")
        else:
            print(f"Unexpected error during {operation}: {str(e)}")

    def _generate_report_url(self, report_date: date, hour: int, period: str) -> str:
        """
        Generate the URL for a specific injury report.

        Args:
            report_date: Date of the report
            hour: Hour of the report (1-12)
            period: Time period ('AM' or 'PM')

        Returns:
            Complete URL for the injury report PDF

        Raises:
            ValidationError: If parameters are invalid
        """
        if not (1 <= hour <= 12):
            raise ValidationError(f"Hour must be between 1 and 12, got {hour}")

        if period.upper() not in ["AM", "PM"]:
            raise ValidationError(f"Period must be 'AM' or 'PM', got {period}")

        # Format: Injury-Report_YYYY-MM-DD_HHPM.pdf
        date_str = report_date.strftime("%Y-%m-%d")
        time_str = f"{hour:02d}{period.upper()}"
        filename = f"Injury-Report_{date_str}_{time_str}.pdf"

        return f"{self.base_url}{filename}"

    def _fetch_report(self, url: str, filename: str) -> Optional[bytes]:
        """
        Fetch a single injury report and return PDF data as bytes.

        Args:
            url: URL of the PDF to fetch
            filename: Filename for logging purposes

        Returns:
            PDF data as bytes if successful, None otherwise
        """
        try:
            print(f"Fetching report from: {url}")

            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            # Check if the response is actually a PDF
            content_type = response.headers.get("content-type", "").lower()
            if (
                "pdf" not in content_type
                and "application/octet-stream" not in content_type
            ):
                print(f"Warning: Expected PDF but got content-type: {content_type}")

            file_size = len(response.content)
            print(f"Successfully fetched: {filename} ({file_size} bytes)")
            return response.content

        except requests.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                if e.response.status_code == 404:
                    print(f"Report not found (404): {filename}")
                else:
                    print(
                        f"HTTP error {e.response.status_code} fetching {filename}: {str(e)}"
                    )
            else:
                print(f"Network error fetching {filename}: {str(e)}")
            return None
        except Exception as e:
            print(f"Unexpected error fetching {filename}: {str(e)}")
            return None

    def _get_current_datetime_info(self) -> Tuple[date, int, str]:
        """
        Get current date and likely report time based on NBA injury report schedule.

        Converts Brazil time to Eastern Time and determines the most recent report.
        NBA injury reports are published hourly.

        Returns:
            Tuple of (date, hour, period) for the most likely current report
        """
        # Get current time in Brazil (UTC-3)
        now_brazil = datetime.now()

        # Convert to Eastern Time based on configured offset
        # EDT (UTC-4): 1 hour difference, EST (UTC-5): 2 hours difference
        now_et = now_brazil - timedelta(hours=self.et_offset_hours)

        et_date = now_et.date()
        et_hour = now_et.hour

        print(
            f"Brazil time: {now_brazil.strftime('%Y-%m-%d %H:%M')} | ET time: {now_et.strftime('%Y-%m-%d %H:%M')}"
        )

        # Convert 24h to 12h format
        if et_hour == 0:
            hour_12 = 12
            period = "AM"
        elif et_hour < 12:
            hour_12 = et_hour
            period = "AM"
        elif et_hour == 12:
            hour_12 = 12
            period = "PM"
        else:
            hour_12 = et_hour - 12
            period = "PM"

        # NBA reports come out hourly, so try the current ET hour first
        print(f"Trying most recent report time: {et_date} {hour_12:02d}{period} ET")
        return et_date, hour_12, period

    def fetch_current_report(self) -> Optional[Tuple[bytes, str]]:
        """
        Fetch the current injury report based on current date and time.

        Attempts to fetch the most recent injury report available based on
        the NBA's typical publishing schedule (usually 6:30 PM ET).

        Returns:
            Tuple of (PDF data as bytes, filename) if successful, None otherwise
        """
        try:
            print("Fetching current injury report...")

            report_date, hour, period = self._get_current_datetime_info()
            url = self._generate_report_url(report_date, hour, period)
            filename = f"current_injury_report_{report_date}_{hour:02d}{period}.pdf"

            pdf_data = self._fetch_report(url, filename)

            if pdf_data is None:
                # Try alternative times if the primary one fails
                print("Primary report not found, trying recent hourly reports...")

                # Try previous hours (reports come out hourly)
                now_brazil = datetime.now()
                current_et = now_brazil - timedelta(hours=self.et_offset_hours)

                # Try last 4 hours in ET
                for hours_back in range(1, 5):
                    try_et = current_et - timedelta(hours=hours_back)
                    try_date = try_et.date()
                    try_hour_24 = try_et.hour

                    # Convert to 12h format
                    if try_hour_24 == 0:
                        try_hour_12 = 12
                        try_period = "AM"
                    elif try_hour_24 < 12:
                        try_hour_12 = try_hour_24 if try_hour_24 != 0 else 12
                        try_period = "AM"
                    elif try_hour_24 == 12:
                        try_hour_12 = 12
                        try_period = "PM"
                    else:
                        try_hour_12 = try_hour_24 - 12
                        try_period = "PM"

                    # Skip if we already tried this one
                    if (
                        try_date == report_date
                        and try_hour_12 == hour
                        and try_period == period
                    ):
                        continue

                    print(
                        f"Trying {hours_back}h ago: {try_date} {try_hour_12:02d}{try_period} ET"
                    )

                    alt_url = self._generate_report_url(
                        try_date, try_hour_12, try_period
                    )
                    alt_filename = f"current_injury_report_{try_date}_{try_hour_12:02d}{try_period}.pdf"

                    alt_pdf_data = self._fetch_report(alt_url, alt_filename)
                    if alt_pdf_data is not None:
                        return (alt_pdf_data, alt_filename)

                # If still no luck, try common NBA report times as final fallback
                print("Trying common NBA report times as final fallback...")
                common_times = [(6, "PM"), (6, "AM"), (12, "PM"), (3, "PM")]
                for alt_hour, alt_period in common_times:
                    if alt_hour == hour and alt_period == period:
                        continue  # Skip if already tried

                    alt_url = self._generate_report_url(
                        report_date, alt_hour, alt_period
                    )
                    alt_filename = f"current_injury_report_{report_date}_{alt_hour:02d}{alt_period}.pdf"

                    alt_pdf_data = self._fetch_report(alt_url, alt_filename)
                    if alt_pdf_data is not None:
                        return (alt_pdf_data, alt_filename)

                return None

            return (pdf_data, filename)

        except Exception as e:
            self._handle_exceptions(e, "current report fetch")
            return None

    def fetch_specific_report(
        self, report_date: date, hour: int, period: str
    ) -> Optional[Tuple[bytes, str]]:
        """
        Fetch a specific injury report by date and time.

        Args:
            report_date: Date of the report
            hour: Hour of the report (1-12)
            period: Time period ('AM' or 'PM')

        Returns:
            Tuple of (PDF data as bytes, filename) if successful, None otherwise

        Raises:
            ValidationError: If parameters are invalid
        """
        try:
            print(
                f"Fetching specific report for {report_date} at {hour:02d}{period}..."
            )

            url = self._generate_report_url(report_date, hour, period)
            filename = f"injury_report_{report_date}_{hour:02d}{period}.pdf"

            pdf_data = self._fetch_report(url, filename)
            if pdf_data is not None:
                return (pdf_data, filename)
            return None

        except Exception as e:
            self._handle_exceptions(e, "specific report fetch")
            return None

    def fetch_historical_reports(
        self,
        start_date: date,
        end_date: date,
        times: Optional[List[Tuple[int, str]]] = None,
    ) -> List[Tuple[bytes, str]]:
        """
        Fetch multiple historical injury reports within a date range.

        Args:
            start_date: Start date for the range (inclusive)
            end_date: End date for the range (inclusive)
            times: List of (hour, period) tuples to try for each date.
                  Defaults to common NBA report times: [(6, 'AM'), (6, 'PM')]

        Returns:
            List of tuples (PDF data as bytes, filename) for successfully fetched reports

        Raises:
            ValidationError: If date range is invalid
        """
        try:
            if start_date > end_date:
                raise ValidationError("Start date must be before or equal to end date")

            if times is None:
                times = [(6, "AM"), (6, "PM")]  # Common NBA injury report times

            print(f"Fetching historical reports from {start_date} to {end_date}")
            print(f"Times to try for each date: {times}")

            successful_fetches = []
            current_date = start_date

            while current_date <= end_date:
                print(f"\nProcessing date: {current_date}")

                for hour, period in times:
                    try:
                        url = self._generate_report_url(current_date, hour, period)
                        filename = (
                            f"injury_report_{current_date}_{hour:02d}{period}.pdf"
                        )

                        pdf_data = self._fetch_report(url, filename)
                        if pdf_data is not None:
                            successful_fetches.append((pdf_data, filename))

                    except Exception as e:
                        print(
                            f"Error fetching report for {current_date} {hour:02d}{period}: {str(e)}"
                        )
                        continue

                current_date += timedelta(days=1)

            print(
                f"\nHistorical fetch complete. Successfully fetched {len(successful_fetches)} reports:"
            )
            for _, filename in successful_fetches:
                print(f"  - {filename}")

            return successful_fetches

        except Exception as e:
            self._handle_exceptions(e, "historical reports fetch")
            return []

    def extract_and_save_current_injury_report(
        self,
        smartbetting_lib,
        bucket_name: str,
        catalog: str,
        schema: str,
        upload_to_gcs: bool = True,
    ) -> bool:
        """
        Extract current injury report and save to GCS.

        Args:
            smartbetting_lib: SmartbettingLib instance for GCS operations
            bucket_name: GCS bucket name
            catalog: Data catalog (e.g., 'injury_report')
            schema: Data schema (e.g., 'landing')
            upload_to_gcs: Whether to upload to GCS

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print("=== Buscando Current Injury Report ===")
            print(f"Iniciado em: {datetime.now()}")

            print("\n1. Buscando relatório atual...")
            result = self.fetch_current_report()

            if result is None:
                print("❌ Nenhum relatório atual disponível")
                return False

            pdf_data, filename = result
            
            # Extrair informações do relatório do filename
            report_info = self._extract_report_info(filename)
            
            print(f"✅ Sucesso: {filename} ({len(pdf_data)} bytes)")
            print(f"📅 Data do relatório: {report_info['date']}")
            print(f"🕐 Hora do relatório: {report_info['time']}")
            print(f"📊 Período: {report_info['period']}")
            print(f"📁 Nome do arquivo: {filename}")

            if upload_to_gcs:
                success = smartbetting_lib.upload_pdf_to_gcs(
                    pdf_data=pdf_data,
                    bucket_name=bucket_name,
                    blob_name=f"{catalog}/{schema}/{filename}",
                )
                if success:
                    print("✅ Upload para GCS concluído com sucesso!")
                    print(f"📍 Localização: gs://{bucket_name}/{catalog}/{schema}/{filename}")
                return success
            else:
                print("ℹ️ Upload para GCS desabilitado")
                return True

        except Exception as e:
            print(f"❌ Erro durante processamento: {str(e)}")
            return False

    def _extract_report_info(self, filename: str) -> dict:
        """
        Extrai informações de data e hora do filename do relatório.
        
        Args:
            filename: Nome do arquivo do relatório
            
        Returns:
            dict: Informações extraídas (date, time, period)
        """
        try:
            import re
            
            # Remover extensão .pdf
            name_without_ext = filename.replace('.pdf', '')
            
            # Padrão para data YYYY-MM-DD
            date_pattern = r'(\d{4}-\d{2}-\d{2})'
            date_match = re.search(date_pattern, name_without_ext)
            
            # Padrão para hora HHMM ou HH:MM
            time_pattern = r'(\d{1,2})(\d{2})?(AM|PM)'
            time_match = re.search(time_pattern, name_without_ext, re.IGNORECASE)
            
            if date_match and time_match:
                report_date = date_match.group(1)
                hour = int(time_match.group(1))
                period = time_match.group(3).upper()
                
                # Formatar hora para exibição
                time_str = f"{hour:02d}{period}"
                
                return {
                    'date': report_date,
                    'time': time_str,
                    'period': period
                }
            else:
                # Se não conseguir extrair, usar informações genéricas
                return {
                    'date': 'Data não identificada',
                    'time': 'Hora não identificada', 
                    'period': 'Período não identificado'
                }
                
        except Exception as e:
            print(f"⚠️ Aviso: Não foi possível extrair informações do filename: {str(e)}")
            return {
                'date': 'Data não identificada',
                'time': 'Hora não identificada',
                'period': 'Período não identificado'
            }


# Example usage (commented out for production)
if __name__ == "__main__":
    """
    Example usage of the NBAInjuryReport class with timezone conversion.
    """
    print("🏀 NBA Injury Report - Timezone-Aware Example")
    print("=" * 50)

    # Example 1: Default (EDT - 1 hour offset)
    print("\n=== Example 1: EDT Mode (default) ===")
    injury_client_edt = NBAInjuryReport(et_offset_hours=1)
    result = injury_client_edt.fetch_current_report()
    if result:
        pdf_data, filename = result
        print(f"✅ EDT report: {filename} ({len(pdf_data)} bytes)")
    else:
        print("❌ No EDT report found")

    # Example 2: EST Mode (2 hour offset)
    print("\n=== Example 2: EST Mode (winter time) ===")
    injury_client_est = NBAInjuryReport(et_offset_hours=2)
    result = injury_client_est.fetch_current_report()
    if result:
        pdf_data, filename = result
        print(f"✅ EST report: {filename} ({len(pdf_data)} bytes)")
    else:
        print("❌ No EST report found")

    # Example 3: Fetch a specific report
    print("\n=== Example 3: Fetch specific report ===")
    from datetime import date

    specific_date = date(2025, 4, 7)  # Example date
    result = injury_client_edt.fetch_specific_report(specific_date, 6, "PM")
    if result:
        pdf_data, filename = result
        print(f"✅ Specific report: {filename} ({len(pdf_data)} bytes)")
    else:
        print("❌ Failed to fetch specific report")

    print("\n" + "=" * 50)
