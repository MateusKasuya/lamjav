"""
Player De-Para Pipeline using Fuzzy String Matching.

This script orchestrates the process of matching players between active players
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
    Main function to execute the player de-para pipeline.

    This function:
    1. Fetches active players from BigQuery staging table
    2. Fetches distinct injury report players from BigQuery staging table
    3. Performs fuzzy string matching between the two datasets
    4. Uploads results to BigQuery in bi_dev.de_para_players

    Returns:
        None
    """
    print("=== PLAYER DE-PARA PIPELINE ===")
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

        print(f"   Active players: {len(active_players)}")
        print(f"   Distinct injury players: {len(injury_players)}")

        # Perform fuzzy string matching
        print("\n3. Performing fuzzy string matching...")
        matches_df = fuzzy_matcher.match_players(
            active_players=active_players,
            injury_players=injury_players,
            threshold=80,  # Minimum similarity score
        )

        # Generate matching report
        print("\n4. Generating matching report...")
        report = fuzzy_matcher.generate_matching_report(matches_df)

        print("📊 MATCHING REPORT:")
        print(f"   Total players: {report['total_players']}")
        print(f"   Matched players: {report['matched_players']}")
        print(f"   Unmatched players: {report['unmatched_players']}")
        print(f"   Match rate: {report['match_rate']}%")
        print(f"   High confidence (≥90): {report['high_confidence_matches']}")
        print(f"   Medium confidence (80-89): {report['medium_confidence_matches']}")
        print(f"   Low confidence (<80): {report['low_confidence_matches']}")

        # Prepare data for upload with specified columns
        print("\n5. Preparing data for upload...")
        upload_df = matches_df[
            [
                "injury_player_name",
                "active_player_name",
                "active_player_id",
                "similarity_score",
            ]
        ].copy()
        upload_df.columns = [
            "injury_player_name",
            "nba_player_name",
            "nba_player_id",
            "similarity_score",
        ]

        # Show sample of results
        print("\n6. Sample of matching results:")
        print("=" * 90)
        print(
            f"{'Injury Player':<25} | {'NBA Player':<25} | {'NBA ID':<8} | {'Score':<5}"
        )
        print("=" * 90)

        for _, row in upload_df.head(10).iterrows():
            injury_name = row["injury_player_name"][:24]
            nba_name = (
                row["nba_player_name"][:24] if row["nba_player_name"] else "NO MATCH"
            )
            nba_id = row["nba_player_id"] if row["nba_player_id"] else "N/A"
            score = row["similarity_score"]
            print(f"{injury_name:<25} | {nba_name:<25} | {nba_id:<8} | {score:<5}")

        # Upload to BigQuery
        print("\n7. Uploading results to BigQuery...")
        dataset_id = "bi_dev"
        table_id = "de_para_players"

        smartbetting.upload_to_bigquery(
            data=upload_df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            write_disposition="WRITE_TRUNCATE",  # Replace existing data
        )

        print("✅ Player de-para pipeline completed successfully!")
        print(f"📊 BigQuery table: {project_id}.{dataset_id}.{table_id}")
        print(f"Completed at: {datetime.now()}")

    except Exception as e:
        print(f"❌ Error in player de-para pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
