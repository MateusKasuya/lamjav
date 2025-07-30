-- External table for NBA Active Players
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://lamjav_storage/nba/landing/active_players/active_players_*.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

CREATE OR REPLACE EXTERNAL TABLE `lamjav.nba.raw_active_players`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://lamjav_storage/nba/landing/active_players/active_players_*.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `lamjav.nba.raw_active_players` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `lamjav.nba.raw_active_players` LIMIT 10;