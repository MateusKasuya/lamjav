-- External table for NBA Games
-- Location: gs://smartbetting-landing/nba/games/
-- Format: NEWLINE_DELIMITED_JSON
-- Defines ONLY the columns we use with explicit types

DROP EXTERNAL TABLE IF EXISTS `sigma-heuristic-469419-h3.nba.raw_games`;

CREATE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_games`
(
  id INT64,
  date DATE,
  home_team_score FLOAT64,
  visitor_team_score FLOAT64,
  home_team STRUCT<
    id INT64,
    full_name STRING,
    abbreviation STRING
  >,
  visitor_team STRUCT<
    id INT64,
    full_name STRING,
    abbreviation STRING
  >
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/games/raw_nba_games_*.json']
);
