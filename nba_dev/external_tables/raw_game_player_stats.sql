-- External table for NBA Game Player Stats
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://lamjav_storage/nba/landing/game_player_stats/game_player_stats_*.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_game_player_stats`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/game_player_stats/2025/raw_nba_game_player_stats_*.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `lamjav.nba.raw_season_averages` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `lamjav.nba.raw_season_averages` LIMIT 10;