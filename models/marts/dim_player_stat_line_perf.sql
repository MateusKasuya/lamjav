{{
  config(
    materialized='table',
    schema='bi',
    description='NBA players stats vs line performance',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH historical_event_odds AS (
    SELECT
        player_name,
        market_key,
        line
    FROM {{ ref('stg_historical_event_odds') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name, market_key ORDER BY snapshot_timestamp DESC) = 1
),

stat_over_line AS (
    SELECT
        gps.player_id,
        gps.stat_type,
        gps.game_number,
        CASE
            WHEN o.line IS null THEN null
            WHEN gps.stat_value >= line THEN 'over'
            WHEN gps.stat_value < line THEN 'under'
        END AS stat_vs_line
    FROM
        {{ ref('int_game_player_stats') }} AS gps
    LEFT JOIN {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para ON gps.player_id = de_para.nba_player_id
    LEFT JOIN historical_event_odds AS o ON de_para.odds_player_name = o.player_name AND gps.stat_type = o.market_key
),

last_30 AS (
    SELECT
        player_id,
        stat_type,
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
        ) * 100 / COUNT(*), 0) AS perc_over_line,
        'L30' AS game_numbers
    FROM
        stat_over_line
    GROUP BY player_id, stat_type
),

last_10 AS (
    SELECT
        player_id,
        stat_type,
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
        ) * 100 / COUNT(*), 0) AS perc_over_line,
        'L10' AS game_numbers
    FROM
        stat_over_line
    WHERE game_number <= 10
    GROUP BY player_id, stat_type
),

last_5 AS (
    SELECT
        player_id,
        stat_type,
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
        ) * 100 / COUNT(*), 0) AS perc_over_line,
        'L5' AS game_numbers
    FROM
        stat_over_line
    WHERE game_number <= 5
    GROUP BY player_id, stat_type
)

SELECT * FROM last_30
UNION ALL
SELECT * FROM last_10
UNION ALL
SELECT * FROM last_5
ORDER BY player_id, game_numbers
