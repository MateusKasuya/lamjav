-- External table for Current Event Odds
-- This table reads raw API responses from The Odds API
-- Location: gs://smartbetting-landing/odds/event_odds/2025/draftkings/
-- Format: NEWLINE_DELIMITED_JSON

-- Note: This table contains current odds data from DraftKings for NBA events
-- Each file represents odds for a specific event at the time of extraction
-- Filename pattern: raw_odds_event_odds_draftkings_{event_id}.json

-- Create external table with auto schema detection
CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.odds.raw_event_odds`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = [
    'gs://smartbetting-landing/odds/event_odds/2025/draftkings/raw_odds_event_odds_draftkings_*.json'
    -- Add more bookmakers as needed:
    -- 'gs://smartbetting-landing/odds/event_odds/2025/fanduel/raw_odds_event_odds_fanduel_*.json',
    -- 'gs://smartbetting-landing/odds/event_odds/2025/betmgm/raw_odds_event_odds_betmgm_*.json'
  ]
);

-- Sample queries to test the external table:
-- SELECT * FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` LIMIT 10;
-- SELECT sport_title, commence_time, COUNT(*) as events FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` GROUP BY sport_title, commence_time LIMIT 10;
-- SELECT DISTINCT bookmaker_key FROM `sigma-heuristic-469419-h3.odds.raw_historical_event_odds` WHERE bookmaker_key IS NOT NULL;
