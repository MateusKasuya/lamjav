{{ config(
    description='Intermediate model that unpivots game player stats - maintains base structure with unpivoted stat columns'
) }}

WITH base_data AS (
    SELECT * FROM {{ ref('stg_game_player_stats') }}
    WHERE
        minutes > 0
        AND game_number <= 30
        AND game_date < '2025-04-07'
),

-- Unpivot the specified stats into long format
stats_unpivoted AS (
    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_points' AS stat_type,
        CAST(points AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_threes' AS stat_type,
        CAST(threes AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_rebounds' AS stat_type,
        CAST(rebounds AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_assists' AS stat_type,
        CAST(assists AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_blocks' AS stat_type,
        CAST(blocks AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_steals' AS stat_type,
        CAST(steals AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_turnovers' AS stat_type,
        CAST(turnovers AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_points_rebounds' AS stat_type,
        CAST(points_rebounds AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_points_assists' AS stat_type,
        CAST(points_assists AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_rebounds_assists' AS stat_type,
        CAST(rebounds_assists AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_points_rebounds_assists' AS stat_type,
        CAST(points_rebounds_assists AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_blocks_steals' AS stat_type,
        CAST(blocks_steals AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_double_double' AS stat_type,
        CAST(double_double AS FLOAT64) AS stat_value
    FROM base_data

    UNION ALL

    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        'player_triple_double' AS stat_type,
        CAST(triple_double AS FLOAT64) AS stat_value
    FROM base_data
)

SELECT
    player_id,
    team_id,
    game_id,
    game_number,
    game_date,
    stat_type,
    stat_value,
    CURRENT_TIMESTAMP() AS loaded_at
FROM stats_unpivoted
