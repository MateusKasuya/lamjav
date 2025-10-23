-- External table for Historical Event Odds
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://smartbetting-landing/odds/historical_event_odds/2024/
-- Format: NEWLINE_DELIMITED_JSON

-- Note: This table contains historical odds data from multiple bookmakers for NBA events
-- Each file represents odds for a specific event on a specific date
-- Filename pattern: raw_odds_historical_event_odds_fanduel_{event_id}_{date}.json

-- Create external table (currently using DraftKings only, as per your Python code)
CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.odds.raw_historical_event_odds`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = [
    'gs://smartbetting-landing/odds/event_odds/2025/draftkings/raw_odds_event_odds_draftkings_*.json'
    -- Add more bookmakers as needed:
    --'gs://smartbetting-landing/odds/historical_event_odds/2024/fanduel/raw_odds_historical_event_odds_fanduel_*.json',
    -- 'gs://smartbetting-landing/odds/historical_event_odds/2024/betmgm/raw_odds_historical_event_odds_betmgm_*.json'
  ]
);

-- Sample queries to test the external table:
-- SELECT * FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` LIMIT 10;
-- SELECT sport_title, commence_time, COUNT(*) as events FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` GROUP BY sport_title, commence_time LIMIT 10;
-- SELECT DISTINCT bookmaker_key FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` WHERE bookmaker_key IS NOT NULL;
