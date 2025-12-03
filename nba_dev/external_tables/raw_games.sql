-- External table for NBA Games
-- Location: gs://smartbetting-landing/nba/games/
-- Format: NEWLINE_DELIMITED_JSON
-- Defines ONLY the columns we use with explicit types

DROP EXTERNAL TABLE IF EXISTS `sigma-heuristic-469419-h3.nba.raw_games`;

CREATE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_games`
(
  id INT64,
  date DATE,
  season STRING,
  status STRING,
  period INT64,
  time STRING,
  postseason BOOL,
  datetime TIMESTAMP,
  home_team_score FLOAT64,
  visitor_team_score FLOAT64,
  home_q1 INT64,
  home_q2 INT64,
  home_q3 INT64,
  home_q4 INT64,
  home_ot1 INT64,
  home_ot2 INT64,
  home_ot3 INT64,
  home_timeouts_remaining INT64,
  home_in_bonus BOOL,
  visitor_q1 INT64,
  visitor_q2 INT64,
  visitor_q3 INT64,
  visitor_q4 INT64,
  visitor_ot1 INT64,
  visitor_ot2 INT64,
  visitor_ot3 INT64,
  visitor_timeouts_remaining INT64,
  visitor_in_bonus BOOL,
  home_team STRUCT<
    id INT64,
    conference STRING,
    division STRING,
    city STRING,
    name STRING,
    full_name STRING,
    abbreviation STRING
  >,
  visitor_team STRUCT<
    id INT64,
    conference STRING,
    division STRING,
    city STRING,
    name STRING,
    full_name STRING,
    abbreviation STRING
  >
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/games/2025/raw_nba_games_*.json']
);
