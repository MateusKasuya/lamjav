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
        game_id,
        home_team_id AS team_id,
        game_date,
        'HOME' AS game_type
    FROM {{ ref('stg_games') }}
    
    UNION ALL
    
    SELECT
        game_id,
        visitor_team_id AS team_id,
        game_date,
        'VISITOR' AS game_type
    FROM {{ ref('stg_games') }}
),

consecutive_games AS (
    SELECT
        team_id,
        game_id,
        game_date,
        game_type,
        LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date) AS previous_game_date,
        LAG(game_id) OVER (PARTITION BY team_id ORDER BY game_date) AS previous_game_id,
        LAG(game_type) OVER (PARTITION BY team_id ORDER BY game_date) AS previous_game_type,
        DATE_DIFF(game_date, LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date), DAY) AS days_between_games
    FROM all_team_games
),

b2b_games AS (
    SELECT
        team_id,
        game_id AS current_game_id,
        game_date AS current_game_date,
        game_type AS current_game_type,
        previous_game_id,
        previous_game_date,
        previous_game_type
    FROM consecutive_games
    WHERE days_between_games = 1
)

SELECT * FROM b2b_games

