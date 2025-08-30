CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_teams`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/teams/raw_nba_teams.json']
);