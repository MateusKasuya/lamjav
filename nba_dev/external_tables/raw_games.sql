-- External table for NBA Games
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://lamjav_storage/nba/landing/games/games_*.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

CREATE OR REPLACE EXTERNAL TABLE `smartbetting.nba.raw_games`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/games/raw_nba_games_*.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `smartbetting.nba.raw_games` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `smartbetting.nba.raw_games` LIMIT 10;