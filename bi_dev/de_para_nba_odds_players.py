"""
NBA x Odds Players De-Para Pipeline using Fuzzy String Matching.

This script orchestrates the process of matching players between NBA active players
and odds data using fuzzy string matching techniques with team context.
"""

import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from lib_dev.fuzzystringmatch import FuzzyStringMatch
from lib_dev.smartbetting import SmartbettingLib


def main():
    """
    Main function to execute the NBA x Odds players de-para pipeline.

    This function:
    1. Fetches NBA active players from BigQuery staging table
    2. Fetches distinct odds players from BigQuery staging table
    3. Performs fuzzy string matching: odds players → NBA active players (with team context)
    4. Uploads results to BigQuery table: bi_dev.de_para_nba_odds_players

    Returns:
        None
    """
    print("=== NBA x ODDS PLAYERS DE-PARA PIPELINE ===")
    print(f"Started at: {datetime.now()}")

    # Load environment variables
    load_dotenv()
    project_id = os.getenv("DBT_PROJECT")

    if not project_id:
        raise ValueError("DBT_PROJECT environment variable not set")

    try:
        # Initialize components
        print("\n1. Initializing components...")
        fuzzy_matcher = FuzzyStringMatch(project_id)
        smartbetting = SmartbettingLib()

        # Fetch data from BigQuery
        print("\n2. Fetching data from BigQuery...")
        active_players = fuzzy_matcher.get_active_players()
        odds_players = fuzzy_matcher.get_odds_players()

        print(f"   NBA Active players: {len(active_players)}")
        print(f"   Odds players: {len(odds_players)}")

        # Check if we have the new team-based columns
        if "last_name_first_team" not in active_players.columns:
            print("\n⚠️  WARNING: Team-based matching not available yet.")
            print("   The staging models need to be updated first.")
            print("   Falling back to basic name matching without team context.")

            # Use basic matching without team context for now
            print("\n3. Performing fuzzy string matching: Odds → NBA (basic mode)...")

            # Create a temporary basic matching function
            from thefuzz import fuzz, process
            import pandas as pd

            matches = []
            active_names = active_players["last_name_first"].tolist()

            for idx, odds_player in odds_players.iterrows():
                player_name = odds_player["player_name"]

                # Find best match against NBA active players
                best_match_nba = process.extractOne(
                    player_name, active_names, scorer=fuzz.token_sort_ratio
                )

                # Initialize match record
                match_record = {
                    "odds_player_name": player_name,
                    "nba_player_name": None,
                    "nba_player_id": None,
                    "similarity_score": 0,
                    "is_confident_match": False,
                }

                if best_match_nba:
                    matched_name_nba, similarity_score_nba = best_match_nba
                    match_record["similarity_score"] = similarity_score_nba

                    if matched_name_nba:
                        # Find the corresponding NBA active player record
                        active_player = active_players[
                            active_players["last_name_first"] == matched_name_nba
                        ].iloc[0]
                        match_record["nba_player_name"] = matched_name_nba
                        match_record["nba_player_id"] = active_player["player_id"]

                        # Flag if it's a confident match based on threshold
                        match_record["is_confident_match"] = similarity_score_nba >= 80

                matches.append(match_record)

            nba_odds_matches = pd.DataFrame(matches)
            nba_odds_matches = nba_odds_matches.sort_values(
                "similarity_score", ascending=False
            )

        else:
            # Use advanced team-based matching
            print(
                "\n3. Performing fuzzy string matching: Odds → NBA (with team context)..."
            )
            nba_odds_matches = fuzzy_matcher.match_nba_odds_players(
                active_players=active_players,
                odds_players=odds_players,
                threshold=80,  # Minimum similarity score
            )

        # Generate matching report
        print("\n4. Generating matching report...")

        odds_report = fuzzy_matcher.generate_matching_report(nba_odds_matches)
        print("📊 NBA x ODDS MATCHING REPORT:")
        print(f"   Total odds players: {odds_report['total_players']}")
        print(
            f"   Confident matches (≥80): {odds_report['confident_matched_players']} ({odds_report['confident_match_rate']}%)"
        )
        print(
            f"   All matches (any score): {odds_report['all_matched_players']} ({odds_report['all_match_rate']}%)"
        )
        print(f"   No matches found: {odds_report['unmatched_players']}")
        print(f"   High confidence (≥90): {odds_report['high_confidence_matches']}")
        print(
            f"   Medium confidence (80-89): {odds_report['medium_confidence_matches']}"
        )
        print(f"   Low confidence (<80): {odds_report['low_confidence_matches']}")

        # Show sample of Odds x NBA results
        print("\n5. Sample of Odds x NBA matching results:")
        print("=" * 95)
        print(
            f"{'Odds Player':<25} | {'NBA Player':<25} | {'NBA ID':<10} | {'Score':<5} | {'Confident':<9}"
        )
        print("=" * 95)

        for _, row in nba_odds_matches.head(10).iterrows():
            odds_name = row["odds_player_name"][:24]
            nba_name = (
                row["nba_player_name"][:24] if row["nba_player_name"] else "NO MATCH"
            )
            nba_id = row["nba_player_id"] if row["nba_player_id"] else "N/A"
            score = row["similarity_score"]
            confident = "✓" if row["is_confident_match"] else "⚠"
            print(
                f"{odds_name:<25} | {nba_name:<25} | {nba_id:<10} | {score:<5} | {confident:<9}"
            )

        # Upload to BigQuery - Odds x NBA table
        print("\n6. Uploading Odds x NBA results to BigQuery...")
        dataset_id = "bi_dev"
        odds_table_id = "de_para_nba_odds_players"

        smartbetting.upload_to_bigquery(
            data=nba_odds_matches,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=odds_table_id,
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
        )

        print("✅ NBA x Odds de-para pipeline completed successfully!")
        print(f"📊 BigQuery table created: {project_id}.{dataset_id}.{odds_table_id}")
        print(f"   Total records: {len(nba_odds_matches)}")
        print(f"   Confident matches: {odds_report['confident_matched_players']}")
        print(f"   Success rate: {odds_report['confident_match_rate']}%")
        print(f"Completed at: {datetime.now()}")

    except Exception as e:
        print(f"❌ Error in NBA x Odds de-para pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
