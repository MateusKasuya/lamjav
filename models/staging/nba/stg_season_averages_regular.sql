{{ config(
    materialized='view',
    description='Staging table for NBA season averages (general/base) from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_season_averages') }}
),

cleaned_data AS (
    SELECT
        player.id AS player_id,
        season,
        TRIM(season_type) AS season_type,

        -- Win/Loss record
        CAST(stats.l AS INTEGER) AS losses,
        CAST(stats.w AS INTEGER) AS wins,
        CAST(stats.w_pct AS FLOAT64) AS win_percentage,

        -- Basic stats
        CAST(stats.gp AS INTEGER) AS games_played,
        CAST(stats.min AS FLOAT64) AS minutes_per_game,
        CAST(stats.pts AS FLOAT64) AS points_per_game,

        -- Shooting stats
        CAST(stats.fgm AS FLOAT64) AS field_goals_made,
        CAST(stats.fga AS FLOAT64) AS field_goals_attempted,
        CAST(stats.fg_pct AS FLOAT64) AS field_goal_percentage,
        CAST(stats.fg3m AS FLOAT64) AS three_pointers_made,
        CAST(stats.fg3a AS FLOAT64) AS three_pointers_attempted,
        CAST(stats.fg3_pct AS FLOAT64) AS three_point_percentage,
        CAST(stats.ftm AS FLOAT64) AS free_throws_made,
        CAST(stats.fta AS FLOAT64) AS free_throws_attempted,
        CAST(stats.ft_pct AS FLOAT64) AS free_throw_percentage,

        -- Rebounding stats
        CAST(stats.oreb AS FLOAT64) AS offensive_rebounds,
        CAST(stats.dreb AS FLOAT64) AS defensive_rebounds,
        CAST(stats.reb AS FLOAT64) AS total_rebounds,

        -- Other stats
        CAST(stats.ast AS FLOAT64) AS assists,
        CAST(stats.stl AS FLOAT64) AS steals,
        CAST(stats.blk AS FLOAT64) AS blocks,
        CAST(stats.tov AS FLOAT64) AS turnovers,
        CAST(stats.pf AS FLOAT64) AS personal_fouls,
        CAST(stats.pfd AS FLOAT64) AS personal_fouls_drawn,
        CAST(stats.blka AS FLOAT64) AS blocks_against,

        -- Advanced stats
        CAST(stats.dd2 AS INTEGER) AS double_doubles,
        CAST(stats.td3 AS INTEGER) AS triple_doubles,
        CAST(stats.plus_minus AS FLOAT64) AS plus_minus,

        -- Rankings (keeping some key ones)
        CAST(stats.pts_rank AS INTEGER) AS points_rank,
        CAST(stats.reb_rank AS INTEGER) AS rebounds_rank,
        CAST(stats.ast_rank AS INTEGER) AS assists_rank,
        CAST(stats.fg_pct_rank AS INTEGER) AS field_goal_percentage_rank,

        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE season_type = 'regular' -- Only regular season data
)

SELECT * FROM cleaned_data
