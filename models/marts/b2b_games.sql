{{
  config(
    materialized='table',
    schema='bi',
    description='NBA B2B games for analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH all_team_games AS (
    -- Unpivot teams to get all games for each team
    SELECT
        g.game_id,
        g.home_team_id AS team_id,
        t.team_abbreviation,
        t.team_full_name,
        g.game_date,
        g.game_datetime_brasilia,
        lta.brasilia_injury_report_time
    FROM {{ ref('stg_games') }} AS g
    INNER JOIN {{ ref('stg_teams') }} AS t ON g.home_team_id = t.team_id
    INNER JOIN {{ ref('stg_local_time_arenas') }} AS lta ON t.team_abbreviation = lta.team_abbreviation

    UNION ALL

    SELECT
        g.game_id,
        g.visitor_team_id AS team_id,
        t.team_abbreviation,
        t.team_full_name,
        g.game_date,
        g.game_datetime_brasilia,
        lta.brasilia_injury_report_time
    FROM {{ ref('stg_games') }} AS g
    INNER JOIN {{ ref('stg_teams') }} AS t ON g.visitor_team_id = t.team_id
    INNER JOIN {{ ref('stg_local_time_arenas') }} AS lta ON t.team_abbreviation = lta.team_abbreviation
),

consecutive_games AS (
    SELECT
        team_abbreviation,
        team_full_name,
        game_id,
        game_date,
        game_datetime_brasilia,
        brasilia_injury_report_time,
        LAG(game_id) OVER (PARTITION BY team_id ORDER BY game_date) AS previous_game_id,
        DATE_DIFF(game_date, LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date), DAY) AS days_between_games
    FROM all_team_games
),

b2b_games AS (
    SELECT
        team_full_name,
        team_abbreviation,
        game_id AS current_game_id,
        game_date AS current_game_date,
        game_datetime_brasilia AS current_game_datetime_brasilia,
        brasilia_injury_report_time AS current_brasilia_injury_report_time,
        previous_game_id,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM consecutive_games
    WHERE days_between_games = 1
)

SELECT * FROM b2b_games
