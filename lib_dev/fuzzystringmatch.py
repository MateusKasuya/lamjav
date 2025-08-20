"""
Fuzzy String Matching utility for player name matching.

This module provides functionality to match player names using fuzzy string matching
techniques to handle variations in naming conventions.
"""

from thefuzz import fuzz
from thefuzz import process
import pandas as pd
from typing import Dict
from google.cloud import bigquery


class FuzzyStringMatch:
    """
    Utility class for fuzzy string matching of player names.

    This class provides methods to match player names from different sources
    using fuzzy string matching algorithms.
    """

    def __init__(self, project_id: str):
        """
        Initialize the FuzzyStringMatch class.

        Args:
            project_id: Google Cloud project ID
        """
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)

    def get_active_players(self) -> pd.DataFrame:
        """
        Fetch active players from BigQuery staging table.

        Returns:
            DataFrame with active players data
        """
        query = """
        SELECT 
            player_id,
            full_name,
            last_name_first,
            team_id,
            position
        FROM `{project_id}.nba.stg_active_players`
        """.format(project_id=self.project_id)

        return self.client.query(query).to_dataframe()

    def get_injury_report_players(self) -> pd.DataFrame:
        """
        Fetch distinct injury report players from BigQuery staging table.

        Returns:
            DataFrame with distinct injury report players data
        """
        query = """
        SELECT DISTINCT
            player_name,
            current_status
        FROM `{project_id}.injury_report.stg_injury_report`
        ORDER BY player_name
        """.format(project_id=self.project_id)

        return self.client.query(query).to_dataframe()

    def match_players(
        self,
        active_players: pd.DataFrame,
        injury_players: pd.DataFrame,
        threshold: int = 80,
    ) -> pd.DataFrame:
        """
        Match players between active players and injury report using fuzzy string matching.

        Args:
            active_players: DataFrame with active players
            injury_players: DataFrame with injury report players
            threshold: Minimum similarity score (0-100) for a match

        Returns:
            DataFrame with matched players and similarity scores
        """
        matches = []

        # Get lists of names for matching
        active_names = active_players["last_name_first"].tolist()
        injury_names = injury_players["player_name"].tolist()

        print(
            f"Matching {len(injury_names)} injury players against {len(active_names)} active players..."
        )

        for idx, injury_player in injury_players.iterrows():
            injury_name = injury_player["player_name"]

            # Find best match using thefuzz
            best_match = process.extractOne(
                injury_name, active_names, scorer=fuzz.token_sort_ratio
            )

            if best_match:
                matched_name, similarity_score = best_match

                if similarity_score >= threshold:
                    # Find the corresponding active player record
                    active_player = active_players[
                        active_players["last_name_first"] == matched_name
                    ].iloc[0]

                    match_record = {
                        "injury_player_name": injury_name,
                        "active_player_name": matched_name,
                        "active_player_id": active_player["player_id"],
                        "similarity_score": similarity_score,
                    }

                    matches.append(match_record)
                else:
                    # No good match found
                    match_record = {
                        "injury_player_name": injury_name,
                        "active_player_name": None,
                        "active_player_id": None,
                        "similarity_score": similarity_score,
                    }

                    matches.append(match_record)

        result_df = pd.DataFrame(matches)

        # Sort by similarity score descending
        result_df = result_df.sort_values("similarity_score", ascending=False)

        return result_df

    def generate_matching_report(self, matches_df: pd.DataFrame) -> Dict[str, int]:
        """
        Generate a summary report of the matching results.

        Args:
            matches_df: DataFrame with matching results

        Returns:
            Dictionary with summary statistics
        """
        total_players = len(matches_df)
        matched_players = len(matches_df[matches_df["active_player_name"].notna()])
        unmatched_players = total_players - matched_players

        # Score distribution
        high_confidence = len(matches_df[matches_df["similarity_score"] >= 90])
        medium_confidence = len(
            matches_df[
                (matches_df["similarity_score"] >= 80)
                & (matches_df["similarity_score"] < 90)
            ]
        )
        low_confidence = len(matches_df[matches_df["similarity_score"] < 80])

        report = {
            "total_players": total_players,
            "matched_players": matched_players,
            "unmatched_players": unmatched_players,
            "match_rate": round((matched_players / total_players) * 100, 2),
            "high_confidence_matches": high_confidence,
            "medium_confidence_matches": medium_confidence,
            "low_confidence_matches": low_confidence,
        }

        return report

    def save_to_csv(self, matches_df: pd.DataFrame, file_path: str) -> None:
        """
        Save matching results to CSV file for validation.

        Args:
            matches_df: DataFrame with matching results
            file_path: Path to save the CSV file
        """
        matches_df.to_csv(file_path, index=False)
        print(f"✅ Matching results saved to: {file_path}")

    def get_unmatched_players(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        """
        Get players that couldn't be matched.

        Args:
            matches_df: DataFrame with matching results

        Returns:
            DataFrame with unmatched players
        """
        return matches_df[matches_df["active_player_name"].isna()]

    def get_low_confidence_matches(
        self, matches_df: pd.DataFrame, threshold: int = 85
    ) -> pd.DataFrame:
        """
        Get matches with low confidence scores for manual review.

        Args:
            matches_df: DataFrame with matching results
            threshold: Minimum confidence threshold

        Returns:
            DataFrame with low confidence matches
        """
        return matches_df[matches_df["similarity_score"] < threshold]
