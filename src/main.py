"""
Main execution script for data ingestion jobs.

This script provides a unified interface to run all ingestion jobs
based on configuration and job type.
"""

import argparse
import sys
import yaml
from datetime import date
from pathlib import Path
from typing import Dict, List, Any

# Add src to path
sys.path.append(str(Path(__file__).parent))

from ingestion.nba.daily import create_daily_job as create_nba_daily_job
from ingestion.nba.intraday import create_intraday_job as create_nba_intraday_job
# from ingestion.odds.daily import create_daily_job as create_odds_daily_job  # To be implemented


def load_config(env: str = "prod") -> Dict[str, Any]:
    """Load configuration for the specified environment."""
    config_path = Path(__file__).parent.parent / "config" / f"{env}.yaml"
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run_daily_jobs(config: Dict[str, Any], catalog: str = None, table: str = None) -> bool:
    """Run daily ingestion jobs."""
    print("🌅 Starting daily jobs execution...")
    
    daily_config = config['schedules']['daily']
    bucket_name = config['bucket_name']
    
    results = []
    
    # Filter jobs based on parameters
    catalogs_to_run = [catalog] if catalog else daily_config['jobs'].keys()
    
    for cat in catalogs_to_run:
        if cat not in daily_config['jobs']:
            print(f"Warning: Catalog {cat} not found in daily jobs config")
            continue
            
        jobs = daily_config['jobs'][cat]
        
        for job_config in jobs:
            job_table = job_config['table']
            
            # Skip if specific table requested and this isn't it
            if table and job_table != table:
                continue
                
            print(f"\n🔄 Running daily job: {cat}/{job_table}")
            
            try:
                # Create appropriate job based on catalog
                if cat == 'nba':
                    job = create_nba_daily_job(
                        table=job_table,
                        bucket_name=bucket_name,
                        api_rate_limit=job_config.get('api_rate_limit', 1.0)
                    )
                    
                    # Execute with any additional parameters
                    params = job_config.get('params', {})
                    success = job.execute(**params)
                    
                elif cat == 'odds':
                    # TODO: Implement odds daily jobs
                    print(f"Odds daily jobs not yet implemented")
                    success = True
                    
                else:
                    print(f"Unknown catalog: {cat}")
                    success = False
                
                results.append((f"{cat}/{job_table}", success))
                
                if success:
                    print(f"✅ {cat}/{job_table} completed successfully")
                else:
                    print(f"❌ {cat}/{job_table} failed")
                    
            except Exception as e:
                print(f"❌ Error running {cat}/{job_table}: {str(e)}")
                results.append((f"{cat}/{job_table}", False))
    
    # Summary
    print(f"\n{'='*60}")
    print("DAILY JOBS SUMMARY")
    print(f"{'='*60}")
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for job_name, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{job_name}: {status}")
    
    print(f"\nOverall: {success_count}/{total_count} jobs successful")
    
    return success_count == total_count


def run_intraday_jobs(config: Dict[str, Any], frequency: str = "medium_frequency", 
                     catalog: str = None, table: str = None) -> bool:
    """Run intraday ingestion jobs."""
    print(f"⚡ Starting intraday jobs execution (frequency: {frequency})...")
    
    intraday_config = config['schedules']['intraday']['schedules'][frequency]
    bucket_name = config['bucket_name']
    
    results = []
    
    # Filter jobs based on parameters
    catalogs_to_run = [catalog] if catalog else intraday_config['jobs'].keys()
    
    for cat in catalogs_to_run:
        if cat not in intraday_config['jobs']:
            print(f"Warning: Catalog {cat} not found in {frequency} jobs config")
            continue
            
        jobs = intraday_config['jobs'][cat]
        
        for job_config in jobs:
            job_table = job_config['table']
            
            # Skip if specific table requested and this isn't it
            if table and job_table != table:
                continue
                
            print(f"\n⚡ Running intraday job: {cat}/{job_table}")
            
            try:
                # Create appropriate job based on catalog
                if cat == 'nba':
                    job = create_nba_intraday_job(
                        table=job_table,
                        bucket_name=bucket_name,
                        api_rate_limit=job_config.get('api_rate_limit', 0.5)
                    )
                    
                    success = job.execute()
                    
                elif cat == 'odds':
                    # TODO: Implement odds intraday jobs
                    print(f"Odds intraday jobs not yet implemented")
                    success = True
                    
                else:
                    print(f"Unknown catalog: {cat}")
                    success = False
                
                results.append((f"{cat}/{job_table}", success))
                
                if success:
                    print(f"✅ {cat}/{job_table} completed successfully")
                else:
                    print(f"❌ {cat}/{job_table} failed")
                    
            except Exception as e:
                print(f"❌ Error running {cat}/{job_table}: {str(e)}")
                results.append((f"{cat}/{job_table}", False))
    
    # Summary
    print(f"\n{'='*60}")
    print(f"INTRADAY JOBS SUMMARY ({frequency})")
    print(f"{'='*60}")
    
    success_count = sum(1 for _, success in results if success)
    total_count = len(results)
    
    for job_name, success in results:
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{job_name}: {status}")
    
    print(f"\nOverall: {success_count}/{total_count} jobs successful")
    
    return success_count == total_count


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Run data ingestion jobs')
    parser.add_argument('--env', default='prod', help='Environment (dev/prod)')
    parser.add_argument('--type', choices=['daily', 'intraday'], required=True, 
                       help='Job type to run')
    parser.add_argument('--catalog', choices=['nba', 'odds'], 
                       help='Specific catalog to run')
    parser.add_argument('--table', help='Specific table to run')
    parser.add_argument('--frequency', choices=['high_frequency', 'medium_frequency'], 
                       default='medium_frequency', help='Frequency for intraday jobs')
    
    args = parser.parse_args()
    
    try:
        # Load configuration
        config = load_config(args.env)
        print(f"🔧 Loaded configuration for environment: {args.env}")
        
        # Run appropriate jobs
        if args.type == 'daily':
            success = run_daily_jobs(config, args.catalog, args.table)
        elif args.type == 'intraday':
            success = run_intraday_jobs(config, args.frequency, args.catalog, args.table)
        else:
            print(f"Unknown job type: {args.type}")
            return 1
        
        if success:
            print(f"\n🎉 All {args.type} jobs completed successfully!")
            return 0
        else:
            print(f"\n💥 Some {args.type} jobs failed. Check logs above.")
            return 1
            
    except Exception as e:
        print(f"💥 Critical error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main()) 