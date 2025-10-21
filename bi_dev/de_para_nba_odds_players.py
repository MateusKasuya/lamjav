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

        # Use advanced team-based matching
        print(
            "\n3. Performing fuzzy string matching: Odds → NBA (with team context)..."
        )
        nba_odds_matches = fuzzy_matcher.match_nba_odds_players(
            active_players=active_players,
            odds_players=odds_players,
            threshold=80,  # Minimum similarity score
        )

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
        odds_table_id = "bi_dev.de_para_nba_odds_players"

        fuzzy_matcher.upload_to_bigquery(
            dataframe=nba_odds_matches,
            table_id=odds_table_id,
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
        )

        print("✅ NBA x Odds de-para pipeline completed successfully!")
        print(f"📊 BigQuery table created: {project_id}.{odds_table_id}")
        print(f"   Total records: {len(nba_odds_matches)}")
        print(f"Completed at: {datetime.now()}")

    except Exception as e:
        print(f"❌ Error in NBA x Odds de-para pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
