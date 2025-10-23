-- External table for NBA Team Standings
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://smartbetting-landing/nba/team_standings/{season}/raw_{catalog}_{table}_{season}.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_team_standings`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/team_standings/2025/raw_nba_team_standings_2025.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `lamjav.nba.raw_team_standings` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `lamjav.nba.raw_team_standings` LIMIT 10;