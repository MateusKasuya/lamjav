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
            last_name_first_team,
            team_id,
            team_full_name,
            team_abbreviation
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
        SELECT
            player_name,
            ANY_VALUE(home_team_abbr) as home_team_abbr,
            ANY_VALUE(away_team_abbr) as away_team_abbr
        FROM
            `{project_id}.odds.stg_historical_event_odds`
        WHERE player_name IS NOT NULL
        GROUP BY player_name
        ORDER BY player_name
        """.format(project_id=self.project_id)

        return self.client.query(query).to_dataframe()

    def match_players(
        self,
        active_players: pd.DataFrame,
        injury_players: pd.DataFrame,
        odds_players: pd.DataFrame = None,
        threshold: int = 80,
    ) -> pd.DataFrame:
        """
        Match injury players against NBA active players using fuzzy string matching.
        Optionally also finds corresponding odds players for reference.

        Args:
            active_players: DataFrame with NBA active players (PRIMARY comparison base)
            injury_players: DataFrame with injury report players
            odds_players: DataFrame with odds players (optional, for additional reference)
            threshold: Minimum similarity score (0-100) for NBA active player match

        Returns:
            DataFrame with matched players and similarity scores
        """
        matches = []

        # Get lists of names for matching
        active_names = active_players["last_name_first"].tolist()
        injury_names = injury_players["player_name"].tolist()
        odds_names = (
            odds_players["last_name_first"].tolist() if odds_players is not None else []
        )

        if odds_players is not None:
            print(
                f"Matching {len(injury_names)} injury players against {len(active_names)} NBA active players (primary) and finding corresponding names in {len(odds_names)} odds players..."
            )
        else:
            print(
                f"Matching {len(injury_names)} injury players against {len(active_names)} NBA active players..."
            )

        for idx, injury_player in injury_players.iterrows():
            injury_name = injury_player["player_name"]

            # PRIMARY MATCHING: Find best match against NBA active players
            best_match_active = process.extractOne(
                injury_name, active_names, scorer=fuzz.token_sort_ratio
            )

            # SECONDARY REFERENCE: Find corresponding name in odds players if available
            best_match_odds = None
            if odds_players is not None and len(odds_names) > 0:
                best_match_odds = process.extractOne(
                    injury_name, odds_names, scorer=fuzz.token_sort_ratio
                )

            # Initialize match record
            match_record = {
                "injury_player_name": injury_name,
                "active_player_name": None,
                "active_player_id": None,
                "similarity_score": 0,
                "odds_player_name": None,
                "odds_similarity_score": 0,
            }

            # Process PRIMARY match against NBA active players
            if best_match_active:
                matched_name_active, similarity_score_active = best_match_active
                match_record["similarity_score"] = similarity_score_active

                if similarity_score_active >= threshold:
                    # Find the corresponding NBA active player record
                    active_player = active_players[
                        active_players["last_name_first"] == matched_name_active
                    ].iloc[0]
                    match_record["active_player_name"] = matched_name_active
                    match_record["active_player_id"] = active_player["player_id"]

            # Process SECONDARY reference from odds players (informational only)
            if best_match_odds:
                matched_name_odds, similarity_score_odds = best_match_odds
                match_record["odds_player_name"] = matched_name_odds
                match_record["odds_similarity_score"] = similarity_score_odds

            matches.append(match_record)

        result_df = pd.DataFrame(matches)

        # Sort by similarity score descending
        result_df = result_df.sort_values("similarity_score", ascending=False)

        return result_df

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
        active_names = active_players["last_name_first"].tolist()
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
                        active_players["last_name_first"] == matched_name_active
                    ].iloc[0]
                    match_record["nba_player_name"] = matched_name_active
                    match_record["nba_player_id"] = active_player["player_id"]

                    # Flag if it's a confident match based on threshold
                    match_record["is_confident_match"] = (
                        similarity_score_active >= threshold
                    )

            matches.append(match_record)

        result_df = pd.DataFrame(matches)

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

        # Create player+team combinations for odds (try both home and away teams)
        odds_names_with_teams = []
        for _, player in odds_players.iterrows():
            player_name = player["player_name"]
            home_team = player["home_team_abbr"]
            away_team = player["away_team_abbr"]

            # Add both possible team combinations
            odds_names_with_teams.append(f"{player_name} ({home_team})")
            odds_names_with_teams.append(f"{player_name} ({away_team})")

        # Remove duplicates
        odds_names_with_teams = list(set(odds_names_with_teams))

        print(
            f"Matching {len(odds_players)} odds players (with team context) against {len(active_names)} NBA active players..."
        )

        for idx, odds_player in odds_players.iterrows():
            player_name = odds_player["player_name"]
            home_team = odds_player["home_team_abbr"]
            away_team = odds_player["away_team_abbr"]

            # Try matching with both possible team combinations
            home_combo = f"{player_name} ({home_team})"
            away_combo = f"{player_name} ({away_team})"

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
                    used_combo = home_combo
                else:
                    best_match_nba = match_away
                    used_combo = away_combo
            elif match_home:
                best_match_nba = match_home
                used_combo = home_combo
            elif match_away:
                best_match_nba = match_away
                used_combo = away_combo
            else:
                best_match_nba = None
                used_combo = home_combo  # fallback

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
                        "last_name_first"
                    ]  # Store without team for consistency
                    match_record["nba_player_id"] = active_player["player_id"]

                    # Flag if it's a confident match based on threshold
                    match_record["is_confident_match"] = (
                        similarity_score_nba >= threshold
                    )

            matches.append(match_record)

        result_df = pd.DataFrame(matches)

        # Sort by similarity score descending
        result_df = result_df.sort_values("similarity_score", ascending=False)

        return result_df

    def generate_injury_matching_report(
        self, matches_df: pd.DataFrame
    ) -> Dict[str, int]:
        """
        Generate a summary report of the NBA x Injury matching results.

        Args:
            matches_df: DataFrame with NBA x Injury matching results

        Returns:
            Dictionary with summary statistics
        """
        total_players = len(matches_df)
        confident_matched_players = len(
            matches_df[matches_df["is_confident_match"] == True]
        )
        all_matched_players = len(matches_df[matches_df["nba_player_name"].notna()])
        unmatched_players = len(matches_df[matches_df["nba_player_name"].isna()])

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
            "confident_matched_players": confident_matched_players,
            "all_matched_players": all_matched_players,
            "unmatched_players": unmatched_players,
            "confident_match_rate": round(
                (confident_matched_players / total_players) * 100, 2
            ),
            "all_match_rate": round((all_matched_players / total_players) * 100, 2),
            "high_confidence_matches": high_confidence,
            "medium_confidence_matches": medium_confidence,
            "low_confidence_matches": low_confidence,
        }

        return report

    def generate_odds_matching_report(self, matches_df: pd.DataFrame) -> Dict[str, int]:
        """
        Generate a summary report of the Odds x NBA matching results.

        Args:
            matches_df: DataFrame with Odds x NBA matching results

        Returns:
            Dictionary with summary statistics
        """
        total_players = len(matches_df)
        confident_matched_players = len(
            matches_df[matches_df["is_confident_match"] == True]
        )
        all_matched_players = len(matches_df[matches_df["nba_player_name"].notna()])
        unmatched_players = len(matches_df[matches_df["nba_player_name"].isna()])

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
            "confident_matched_players": confident_matched_players,
            "all_matched_players": all_matched_players,
            "unmatched_players": unmatched_players,
            "confident_match_rate": round(
                (confident_matched_players / total_players) * 100, 2
            ),
            "all_match_rate": round((all_matched_players / total_players) * 100, 2),
            "high_confidence_matches": high_confidence,
            "medium_confidence_matches": medium_confidence,
            "low_confidence_matches": low_confidence,
        }

        return report

    def generate_matching_report(self, matches_df: pd.DataFrame) -> Dict[str, int]:
        """
        Generate a summary report of the matching results (legacy method).
        Automatically detects the type based on column names.

        Args:
            matches_df: DataFrame with matching results

        Returns:
            Dictionary with summary statistics
        """
        if "injury_player_name" in matches_df.columns:
            return self.generate_injury_matching_report(matches_df)
        elif "odds_player_name" in matches_df.columns:
            return self.generate_odds_matching_report(matches_df)
        else:
            # Fallback for old format
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
