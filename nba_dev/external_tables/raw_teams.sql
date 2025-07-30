CREATE OR REPLACE EXTERNAL TABLE `lamjav.nba.raw_teams`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://lamjav_storage/nba/landing/teams/teams_*.json']
);