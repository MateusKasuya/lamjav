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
        team.id AS team_id,
        game.id AS game_id,
        game.date AS game_date,
        CAST(pts AS INTEGER) AS points,
        CAST(min AS INTEGER) AS minutes,
        CAST(fg3m AS INTEGER) AS threes,
        CAST(reb AS INTEGER) AS rebounds,
        CAST(pts AS INTEGER) + CAST(reb AS INTEGER) AS points_rebounds,
        CAST(ast AS INTEGER) AS assists,
        CAST(pts AS INTEGER) + CAST(ast AS INTEGER) AS points_assists,
        CAST(reb AS INTEGER) + CAST(ast AS INTEGER) AS rebounds_assists,
        CAST(pts AS INTEGER) + CAST(reb AS INTEGER) + CAST(ast AS INTEGER) AS points_rebounds_assists,
        CAST(stl AS INTEGER) AS steals,
        CAST(blk AS INTEGER) AS blocks,
        CAST(blk AS INTEGER) + CAST(stl AS INTEGER) AS blocks_steals,
        CAST(turnover AS INTEGER) AS turnovers,
        CASE
            WHEN (
                (CAST(CAST(pts AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(reb AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(ast AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(stl AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(blk AS INTEGER) > 10 AS INT64))
            ) >= 3 THEN 1
            ELSE 0
        END AS triple_double,
        CASE
            WHEN (
                (CAST(CAST(pts AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(reb AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(ast AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(stl AS INTEGER) > 10 AS INT64))
                + (CAST(CAST(blk AS INTEGER) > 10 AS INT64))
            ) >= 2 THEN 1
            ELSE 0
        END AS double_double,
        ROW_NUMBER() OVER (PARTITION BY player.id ORDER BY game.date DESC) AS game_number,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE game.date < '2025-04-07'
)

SELECT * FROM cleaned_data
