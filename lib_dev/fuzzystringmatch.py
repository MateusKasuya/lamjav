"""
Fuzzy String Matching utility for player name matching.

This module provides functionality to match player names using fuzzy string matching
techniques to handle variations in naming conventions.
"""

from thefuzz import fuzz
from thefuzz import process
import pandas as pd
from google.cloud import bigquery
from datetime import datetime


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
            id AS player_id,
            name,
            last_name_first_team
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
            player_name
        FROM `{project_id}.injury_report.stg_injury_report`
        ORDER BY player_name
        """.format(project_id=self.project_id)

        return self.client.query(query).to_dataframe()

    def get_odds_players(self) -> pd.DataFrame:
        """
        Fetch distinct odds players from BigQuery staging table.

        Returns:
            DataFrame with distinct odds players data
        """
        query = """
        WITH ranked_odds AS (
            SELECT
                player_name,
                player_name_home_team,
                player_name_away_team,
                commence_time,
                -- Rank by most recent snapshot to get current team context
                ROW_NUMBER() OVER (
                    PARTITION BY player_name 
                    ORDER BY commence_time DESC
                ) as rn
            FROM
                `{project_id}.odds.stg_event_odds`
        )
        SELECT
            player_name,
            player_name_home_team,
            player_name_away_team
        FROM ranked_odds
        WHERE rn = 1
        """.format(project_id=self.project_id)

        return self.client.query(query).to_dataframe()

    def match_nba_injury_players(
        self,
        active_players: pd.DataFrame,
        injury_players: pd.DataFrame,
        threshold: int = 80,
    ) -> pd.DataFrame:
        """
        Match injury players against NBA active players using fuzzy string matching.

        Args:
            active_players: DataFrame with NBA active players
            injury_players: DataFrame with injury report players
            threshold: Minimum similarity score (0-100) for a match

        Returns:
            DataFrame with matched NBA and injury players
        """
        matches = []

        # Get lists of names for matching
        active_names = active_players["name"].tolist()
        injury_names = injury_players["player_name"].tolist()

        print(
            f"Matching {len(injury_names)} injury players against {len(active_names)} NBA active players..."
        )

        for idx, injury_player in injury_players.iterrows():
            injury_name = injury_player["player_name"]

            # Find best match against NBA active players
            best_match_active = process.extractOne(
                injury_name, active_names, scorer=fuzz.token_sort_ratio
            )

            # Initialize match record
            match_record = {
                "injury_player_name": injury_name,
                "nba_player_name": None,
                "nba_player_id": None,
                "similarity_score": 0,
                "is_confident_match": False,
            }

            # Process NBA active player match
            if best_match_active:
                matched_name_active, similarity_score_active = best_match_active
                match_record["similarity_score"] = similarity_score_active

                # Always show the best match, regardless of threshold
                if matched_name_active:
                    # Find the corresponding NBA active player record
                    active_player = active_players[
                        active_players["name"] == matched_name_active
                    ].iloc[0]
                    match_record["nba_player_name"] = matched_name_active
                    match_record["nba_player_id"] = active_player["player_id"]

                    # Flag if it's a confident match based on threshold
                    match_record["is_confident_match"] = (
                        similarity_score_active >= threshold
                    )

            matches.append(match_record)

        result_df = pd.DataFrame(matches)

        result_df["extraction_timestamp"] = datetime.now()

        # Sort by similarity score descending
        result_df = result_df.sort_values("similarity_score", ascending=False)

        return result_df

    def match_nba_odds_players(
        self,
        active_players: pd.DataFrame,
        odds_players: pd.DataFrame,
        threshold: int = 80,
    ) -> pd.DataFrame:
        """
        Match odds players against NBA active players using fuzzy string matching.

        Args:
            active_players: DataFrame with NBA active players
            odds_players: DataFrame with odds players
            threshold: Minimum similarity score (0-100) for a match

        Returns:
            DataFrame with matched odds and NBA players
        """
        matches = []

        # Get lists of names for matching with team information
        active_names = active_players[
            "last_name_first_team"
        ].tolist()  # "James, LeBron (LAL)"

        # Use the already concatenated player+team combinations from staging
        odds_names_with_teams = []
        for _, player in odds_players.iterrows():
            home_team_combo = player["player_name_home_team"]
            away_team_combo = player["player_name_away_team"]

            # Add both possible team combinations (already concatenated in staging)
            odds_names_with_teams.append(home_team_combo)
            odds_names_with_teams.append(away_team_combo)

        # Remove duplicates
        odds_names_with_teams = list(set(odds_names_with_teams))

        print(
            f"Matching {len(odds_players)} odds players (with team context) against {len(active_names)} NBA active players..."
        )

        for idx, odds_player in odds_players.iterrows():
            player_name = odds_player["player_name"]

            # Use the already concatenated player+team combinations from staging
            home_combo = odds_player["player_name_home_team"]
            away_combo = odds_player["player_name_away_team"]

            # Find best match for home team combination
            match_home = process.extractOne(
                home_combo, active_names, scorer=fuzz.token_sort_ratio
            )

            # Find best match for away team combination
            match_away = process.extractOne(
                away_combo, active_names, scorer=fuzz.token_sort_ratio
            )

            # Choose the best match between home and away
            if match_home and match_away:
                if match_home[1] >= match_away[1]:
                    best_match_nba = match_home
                else:
                    best_match_nba = match_away
            elif match_home:
                best_match_nba = match_home
            elif match_away:
                best_match_nba = match_away
            else:
                best_match_nba = None

            # Initialize match record
            match_record = {
                "odds_player_name": player_name,  # Store original player name (without team)
                "nba_player_name": None,
                "nba_player_id": None,
                "similarity_score": 0,
                "is_confident_match": False,
            }

            # Process NBA active player match
            if best_match_nba:
                matched_name_nba_with_team, similarity_score_nba = best_match_nba
                match_record["similarity_score"] = similarity_score_nba

                # Always show the best match, regardless of threshold
                if matched_name_nba_with_team:
                    # Find the corresponding NBA active player record using team-based match
                    active_player = active_players[
                        active_players["last_name_first_team"]
                        == matched_name_nba_with_team
                    ].iloc[0]
                    match_record["nba_player_name"] = active_player[
                        "name"
                    ]  # Store without team for consistency
                    match_record["nba_player_id"] = active_player["player_id"]

                    # Flag if it's a confident match based on threshold
                    match_record["is_confident_match"] = (
                        similarity_score_nba >= threshold
                    )

            matches.append(match_record)

        result_df = pd.DataFrame(matches)

        result_df["extraction_timestamp"] = datetime.now()

        # Remove duplicates based on odds_player_name, keeping the best match
        result_df = result_df.sort_values("similarity_score", ascending=False)
        result_df = result_df.drop_duplicates(subset=["odds_player_name"], keep="first")

        # Sort by similarity score descending again after deduplication
        result_df = result_df.sort_values("similarity_score", ascending=False)

        return result_df

    def generate_matching_report(self, matches_df: pd.DataFrame) -> dict:
        """
        Generate a comprehensive matching report from the matches DataFrame.

        Args:
            matches_df: DataFrame with matching results

        Returns:
            Dictionary with matching statistics
        """
        if matches_df.empty:
            return {
                "total_players": 0,
                "confident_matched_players": 0,
                "confident_match_rate": 0.0,
                "all_matched_players": 0,
                "all_match_rate": 0.0,
                "unmatched_players": 0,
                "high_confidence_matches": 0,
                "medium_confidence_matches": 0,
                "low_confidence_matches": 0,
            }

        total_players = len(matches_df)

        # Confident matches (≥80% similarity)
        confident_matches = matches_df[matches_df["is_confident_match"]]
        confident_matched_players = len(confident_matches)
        confident_match_rate = (
            (confident_matched_players / total_players) * 100
            if total_players > 0
            else 0
        )

        # All matches (any similarity score > 0)
        all_matches = matches_df[matches_df["similarity_score"] > 0]
        all_matched_players = len(all_matches)
        all_match_rate = (
            (all_matched_players / total_players) * 100 if total_players > 0 else 0
        )

        # Unmatched players
        unmatched_players = total_players - all_matched_players

        # Confidence level breakdown
        high_confidence_matches = len(matches_df[matches_df["similarity_score"] >= 90])
        medium_confidence_matches = len(
            matches_df[
                (matches_df["similarity_score"] >= 80)
                & (matches_df["similarity_score"] < 90)
            ]
        )
        low_confidence_matches = len(matches_df[matches_df["similarity_score"] < 80])

        return {
            "total_players": total_players,
            "confident_matched_players": confident_matched_players,
            "confident_match_rate": round(confident_match_rate, 2),
            "all_matched_players": all_matched_players,
            "all_match_rate": round(all_match_rate, 2),
            "unmatched_players": unmatched_players,
            "high_confidence_matches": high_confidence_matches,
            "medium_confidence_matches": medium_confidence_matches,
            "low_confidence_matches": low_confidence_matches,
        }

    def upload_to_bigquery(
        self,
        dataframe: pd.DataFrame,
        table_id: str,
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> None:
        """
        Upload DataFrame to BigQuery table.

        Args:
            dataframe: DataFrame to upload
            table_id: BigQuery table ID in format 'dataset.table'
            write_disposition: Write mode ('WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY')
        """
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        full_table_id = f"{self.project_id}.{table_id}"

        job = self.client.load_table_from_dataframe(
            dataframe, full_table_id, job_config=job_config
        )

        job.result()  # Wait for the job to complete

        print(f"✅ Uploaded {len(dataframe)} rows to {full_table_id}")
