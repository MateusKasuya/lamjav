-- External table for NBA Season Averages
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://lamjav_storage/nba/landing/season_averages/season_averages_*.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

CREATE OR REPLACE EXTERNAL TABLE `lamjav.nba.raw_season_averages`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://lamjav_storage/nba/landing/season_averages/season_averages_*.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `lamjav.nba.raw_season_averages` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `lamjav.nba.raw_season_averages` LIMIT 10;