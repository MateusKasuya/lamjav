{{
  config(
    materialized='table',
    schema='bi',
    description='NBA prop player analysis',
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
)

SELECT
    p.id AS player_id,
    s.stat_type,
    s.stat_value,
    o.line,
    CASE
        WHEN o.market_key = 'player_double_double' OR o.market_key = 'player_triple_double' THEN null
        ELSE s.stat_value - o.line
    END AS delta
FROM
    {{ ref('stg_active_players') }} AS p
LEFT JOIN
    {{ ref('int_season_averages_general_base') }} AS s
    ON p.id = s.player_id
LEFT JOIN
    {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para
    ON p.id = de_para.nba_player_id
LEFT JOIN historical_event_odds AS o
    ON
        de_para.odds_player_name = o.player_name
        AND s.stat_type = o.market_key
ORDER BY
    p.team_abbreviation,
    p.name
