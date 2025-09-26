"""
Master Player De-Para Pipeline.

This script executes both NBA x Injury and NBA x Odds de-para pipelines
by calling the individual scripts in sequence.
"""

import sys
import os
from datetime import datetime
import subprocess


def main():
    """
    Main function to execute both de-para pipelines.

    This function runs:
    1. NBA x Injury de-para pipeline
    2. NBA x Odds de-para pipeline

    Returns:
        None
    """
    print("=== MASTER PLAYER DE-PARA PIPELINE ===")
    print(f"Started at: {datetime.now()}")

    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        # Run NBA x Injury pipeline
        print("\n🔄 Running NBA x Injury de-para pipeline...")
        print("=" * 50)

        injury_script = os.path.join(current_dir, "de_para_nba_injury_players.py")
        result_injury = subprocess.run(
            [sys.executable, injury_script], capture_output=False, text=True
        )

        if result_injury.returncode != 0:
            print(
                f"❌ NBA x Injury pipeline failed with return code: {result_injury.returncode}"
            )
            return

        print("\n✅ NBA x Injury pipeline completed successfully!")

        # Run NBA x Odds pipeline
        print("\n🔄 Running NBA x Odds de-para pipeline...")
        print("=" * 50)

        odds_script = os.path.join(current_dir, "de_para_nba_odds_players.py")
        result_odds = subprocess.run(
            [sys.executable, odds_script], capture_output=False, text=True
        )

        if result_odds.returncode != 0:
            print(
                f"❌ NBA x Odds pipeline failed with return code: {result_odds.returncode}"
            )
            return

        print("\n✅ NBA x Odds pipeline completed successfully!")

        print("\n🎉 MASTER PIPELINE COMPLETED SUCCESSFULLY!")
        print("📊 Both BigQuery tables have been updated:")
        print("   - bi_dev.de_para_nba_injury_players")
        print("   - bi_dev.de_para_nba_odds_players")
        print(f"Completed at: {datetime.now()}")

    except Exception as e:
        print(f"❌ Error in master de-para pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    main()
