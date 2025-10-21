"""
Simple PDF Table Extractor for Cloud Functions.

This module provides a simplified PDF table extraction that works in Google Cloud Functions
using pdfplumber instead of camelot-py.
"""

import pandas as pd
import pdfplumber
from typing import List, Dict, Any, Optional
import re


class PDFTableExtractor:
    """
    Simple PDF table extractor for Cloud Functions.
    
    Uses pdfplumber instead of camelot-py to avoid system dependencies.
    """

    def __init__(self, pdf_path: str):
        """
        Initialize the PDF table extractor.
        
        Args:
            pdf_path: Path to the PDF file
        """
        self.pdf_path = pdf_path
        self.pdf = None

    def __enter__(self):
        """Context manager entry."""
        self.pdf = pdfplumber.open(self.pdf_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.pdf:
            self.pdf.close()

    def get_all_players_from_pdf(self) -> pd.DataFrame:
        """
        Extract all player data from the PDF using pdfplumber.
        
        Returns:
            DataFrame with player information
        """
        if not self.pdf:
            raise ValueError("PDF not opened. Use context manager or call open_pdf() first.")

        all_players = []
        
        for page_num, page in enumerate(self.pdf.pages, 1):
            print(f"Processing page {page_num}...")
            
            # Extract tables from the page
            tables = page.extract_tables()
            
            if not tables:
                print(f"No tables found on page {page_num}")
                continue
                
            for table_num, table in enumerate(tables, 1):
                print(f"Processing table {table_num} on page {page_num}")
                
                # Convert table to DataFrame
                if len(table) < 2:  # Need at least header + 1 row
                    print(f"Table {table_num} too small, skipping")
                    continue
                    
                df = pd.DataFrame(table[1:], columns=table[0])
                
                # Clean the DataFrame
                df = self._clean_dataframe(df)
                
                if not df.empty:
                    all_players.append(df)
                    print(f"Extracted {len(df)} players from table {table_num}")

        if not all_players:
            print("No player data found in PDF")
            return pd.DataFrame()

        # Combine all DataFrames
        combined_df = pd.concat(all_players, ignore_index=True)
        
        # Remove duplicates and clean
        combined_df = combined_df.drop_duplicates()
        combined_df = combined_df.reset_index(drop=True)
        
        print(f"Total players extracted: {len(combined_df)}")
        return combined_df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize the DataFrame.
        
        Args:
            df: Raw DataFrame from PDF
            
        Returns:
            Cleaned DataFrame
        """
        if df.empty:
            return df
            
        # Remove completely empty rows
        df = df.dropna(how='all')
        
        # Clean column names
        df.columns = [self._clean_column_name(col) for col in df.columns]
        
        # Remove rows where all values are None or empty
        df = df.dropna(how='all')
        
        # Clean string values
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('None', '')
                df[col] = df[col].replace('nan', '')
        
        return df

    def _clean_column_name(self, col_name: str) -> str:
        """
        Clean column name to standard format.
        
        Args:
            col_name: Raw column name
            
        Returns:
            Cleaned column name
        """
        if not col_name or pd.isna(col_name):
            return "unknown_column"
            
        # Convert to string and clean
        col_name = str(col_name).strip().lower()
        
        # Replace spaces and special characters with underscores
        col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
        
        # Remove multiple underscores
        col_name = re.sub(r'_+', '_', col_name)
        
        # Remove leading/trailing underscores
        col_name = col_name.strip('_')
        
        # Map common variations to standard names
        column_mapping = {
            'player': 'player_name',
            'name': 'player_name',
            'player_name': 'player_name',
            'status': 'current_status',
            'injury_status': 'current_status',
            'current_status': 'current_status',
            'team': 'team',
            'team_name': 'team',
        }
        
        return column_mapping.get(col_name, col_name)

    def sanitize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize column names for BigQuery compatibility.
        
        Args:
            df: DataFrame to sanitize
            
        Returns:
            DataFrame with sanitized column names
        """
        if df.empty:
            return df
            
        # Create a copy to avoid modifying the original
        df_clean = df.copy()
        
        # Clean column names
        new_columns = []
        for col in df_clean.columns:
            # Convert to lowercase and replace spaces with underscores
            clean_col = str(col).lower().replace(' ', '_')
            
            # Remove special characters except underscores
            clean_col = re.sub(r'[^a-zA-Z0-9_]', '', clean_col)
            
            # Ensure it starts with a letter or underscore
            if clean_col and not clean_col[0].isalpha() and clean_col[0] != '_':
                clean_col = 'col_' + clean_col
                
            # Ensure it's not empty
            if not clean_col:
                clean_col = 'unknown_column'
                
            new_columns.append(clean_col)
        
        df_clean.columns = new_columns
        return df_clean

    def open_pdf(self):
        """Open the PDF file."""
        if self.pdf:
            self.pdf.close()
        self.pdf = pdfplumber.open(self.pdf_path)

    def close_pdf(self):
        """Close the PDF file."""
        if self.pdf:
            self.pdf.close()
            self.pdf = None
