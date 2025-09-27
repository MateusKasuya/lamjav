{{ config(
    materialized='view',
    description='Staging table for NBA game player stats from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_game_player_stats') }}
),

cleaned_data AS (
    SELECT
        player.id AS player_id,

        -- Player information
        --team.id AS team_id,

        -- Team information
        --game.id AS game_id,
        game.date AS game_date,
        --game.season AS game_season,

        -- Game information
        --CAST(id AS INTEGER) AS stat_id,

        -- Game stats - Basic
        --CAST(min AS INTEGER) AS minutes_played_int,
        --CAST(pts AS INTEGER) AS points,
        CAST(min AS INTEGER) AS minutes,

        -- Shooting stats
        --CAST(fgm AS INTEGER) AS field_goals_made,
        --CAST(fga AS INTEGER) AS field_goals_attempted,
        --CAST(fg_pct AS FLOAT64) AS field_goal_percentage,
        --CAST(fg3m AS INTEGER) AS three_pointers_made,
        --CAST(fg3a AS INTEGER) AS three_pointers_attempted,
        --CAST(fg3_pct AS FLOAT64) AS three_point_percentage,
        --CAST(ftm AS INTEGER) AS free_throws_made,
        --CAST(fta AS INTEGER) AS free_throws_attempted,
        --CAST(ft_pct AS FLOAT64) AS free_throw_percentage,

        -- Rebounding stats
        --CAST(oreb AS INTEGER) AS offensive_rebounds,
        --CAST(dreb AS INTEGER) AS defensive_rebounds,
        --CAST(reb AS INTEGER) AS total_rebounds,

        -- Other stats
        --CAST(ast AS INTEGER) AS assists,
        --CAST(stl AS INTEGER) AS steals,
        --CAST(blk AS INTEGER) AS blocks,
        --CAST(turnover AS INTEGER) AS turnovers,
        --CAST(pf AS INTEGER) AS personal_fouls,

        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE game.date < '2025-04-07'
)

SELECT * FROM cleaned_data
