{{
  config(
    description='NBA players stats vs line performance',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH historical_event_odds AS (
    SELECT
        player_name,
        market_key,
        line
    FROM {{ ref('stg_event_odds') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name, market_key ORDER BY commence_time DESC) = 1
),

-- Get game player stats with game_number_group from staging
game_player_stats_with_groups AS (
    SELECT
        player_id,
        team_id,
        game_id,
        game_number,
        game_date,
        game_number_group,
        points,
        threes,
        rebounds,
        assists,
        blocks,
        steals,
        turnovers,
        points_rebounds,
        points_assists,
        rebounds_assists,
        points_rebounds_assists,
        blocks_steals,
        double_double,
        triple_double
    FROM {{ ref('stg_game_player_stats') }}
    WHERE
        minutes > 0
),

-- Unpivot stats with game_number_group
stats_unpivoted AS (
    SELECT
        unpivoted.player_id,
        unpivoted.stat_type,
        unpivoted.game_number_group,
        unpivoted.stat_value,
        o.line,
        CASE
            WHEN o.line IS null THEN null
            WHEN unpivoted.stat_value >= o.line THEN 'over'
            WHEN unpivoted.stat_value < o.line THEN 'under'
        END AS stat_vs_line
    FROM (
        SELECT
            gps.player_id,
            'player_points' AS stat_type,
            gps.game_number_group,
            CAST(gps.points AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_threes' AS stat_type,
            gps.game_number_group,
            CAST(gps.threes AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_rebounds' AS stat_type,
            gps.game_number_group,
            CAST(gps.rebounds AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_assists' AS stat_type,
            gps.game_number_group,
            CAST(gps.assists AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_blocks' AS stat_type,
            gps.game_number_group,
            CAST(gps.blocks AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_steals' AS stat_type,
            gps.game_number_group,
            CAST(gps.steals AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_turnovers' AS stat_type,
            gps.game_number_group,
            CAST(gps.turnovers AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_points_rebounds' AS stat_type,
            gps.game_number_group,
            CAST(gps.points_rebounds AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_points_assists' AS stat_type,
            gps.game_number_group,
            CAST(gps.points_assists AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_rebounds_assists' AS stat_type,
            gps.game_number_group,
            CAST(gps.rebounds_assists AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_points_rebounds_assists' AS stat_type,
            gps.game_number_group,
            CAST(gps.points_rebounds_assists AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_blocks_steals' AS stat_type,
            gps.game_number_group,
            CAST(gps.blocks_steals AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_double_double' AS stat_type,
            gps.game_number_group,
            CAST(gps.double_double AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps

        UNION ALL

        SELECT
            gps.player_id,
            'player_triple_double' AS stat_type,
            gps.game_number_group,
            CAST(gps.triple_double AS FLOAT64) AS stat_value
        FROM game_player_stats_with_groups AS gps
    ) AS unpivoted
    LEFT JOIN
        {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para
        ON unpivoted.player_id = de_para.nba_player_id
    LEFT JOIN
        historical_event_odds AS o
        ON de_para.odds_player_name = o.player_name AND unpivoted.stat_type = o.market_key
),

-- Calculate performance by game_number_group
performance_by_group AS (
    SELECT
        player_id,
        stat_type,
        game_number_group,
        SUM(
            CASE
                WHEN stat_vs_line = 'over' THEN 1
                ELSE 0
            END
        ) AS over_lines,
        COUNT(*) AS totals,
        ROUND(SUM(
            CASE
                WHEN stat_vs_line = 'over' THEN 1
                ELSE 0
            END
        ) * 100 / COUNT(*), 0) AS perc_over_line
    FROM stats_unpivoted
    WHERE game_number_group IS NOT null
    GROUP BY player_id, stat_type, game_number_group
)

SELECT
    player_id,
    stat_type,
    over_lines,
    totals,
    perc_over_line,
    game_number_group AS game_numbers
FROM performance_by_group
ORDER BY player_id, game_numbers
