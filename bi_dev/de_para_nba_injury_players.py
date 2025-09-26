"""
NBA x Injury Players De-Para Pipeline using Fuzzy String Matching.

This script orchestrates the process of matching players between NBA active players
and injury report using fuzzy string matching techniques.
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
    Main function to execute the NBA x Injury players de-para pipeline.

    This function:
    1. Fetches NBA active players from BigQuery staging table
    2. Fetches distinct injury report players from BigQuery staging table
    3. Performs fuzzy string matching: injury players → NBA active players
    4. Uploads results to BigQuery table: bi_dev.de_para_nba_injury_players

    Returns:
        None
    """
    print("=== NBA x INJURY PLAYERS DE-PARA PIPELINE ===")
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
        injury_players = fuzzy_matcher.get_injury_report_players()

        print(f"   NBA Active players: {len(active_players)}")
        print(f"   Injury report players: {len(injury_players)}")

        # Perform fuzzy string matching for NBA x Injury
        print("\n3. Performing fuzzy string matching: Injury → NBA...")
        nba_injury_matches = fuzzy_matcher.match_nba_injury_players(
            active_players=active_players,
            injury_players=injury_players,
            threshold=80,  # Minimum similarity score
        )

        # Generate matching report
        print("\n4. Generating matching report...")

        injury_report = fuzzy_matcher.generate_matching_report(nba_injury_matches)
        print("📊 NBA x INJURY MATCHING REPORT:")
        print(f"   Total injury players: {injury_report['total_players']}")
        print(
            f"   Confident matches (≥80): {injury_report['confident_matched_players']} ({injury_report['confident_match_rate']}%)"
        )
        print(
            f"   All matches (any score): {injury_report['all_matched_players']} ({injury_report['all_match_rate']}%)"
        )
        print(f"   No matches found: {injury_report['unmatched_players']}")
        print(f"   High confidence (≥90): {injury_report['high_confidence_matches']}")
        print(
            f"   Medium confidence (80-89): {injury_report['medium_confidence_matches']}"
        )
        print(f"   Low confidence (<80): {injury_report['low_confidence_matches']}")

        # Show sample of NBA x Injury results
        print("\n5. Sample of NBA x Injury matching results:")
        print("=" * 95)
        print(
            f"{'Injury Player':<25} | {'NBA Player':<25} | {'NBA ID':<10} | {'Score':<5} | {'Confident':<9}"
        )
        print("=" * 95)

        for _, row in nba_injury_matches.head(10).iterrows():
            injury_name = row["injury_player_name"][:24]
            nba_name = (
                row["nba_player_name"][:24] if row["nba_player_name"] else "NO MATCH"
            )
            nba_id = row["nba_player_id"] if row["nba_player_id"] else "N/A"
            score = row["similarity_score"]
            confident = "✓" if row["is_confident_match"] else "⚠"
            print(
                f"{injury_name:<25} | {nba_name:<25} | {nba_id:<10} | {score:<5} | {confident:<9}"
            )

        # Upload to BigQuery - NBA x Injury table
        print("\n6. Uploading NBA x Injury results to BigQuery...")
        dataset_id = "bi_dev"
        injury_table_id = "de_para_nba_injury_players"

        smartbetting.upload_to_bigquery(
            data=nba_injury_matches,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=injury_table_id,
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
        )

        print("✅ NBA x Injury de-para pipeline completed successfully!")
        print(f"📊 BigQuery table created: {project_id}.{dataset_id}.{injury_table_id}")
        print(f"   Total records: {len(nba_injury_matches)}")
        print(f"   Confident matches: {injury_report['confident_matched_players']}")
        print(f"   Success rate: {injury_report['confident_match_rate']}%")
        print(f"Completed at: {datetime.now()}")

    except Exception as e:
        print(f"❌ Error in NBA x Injury de-para pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
