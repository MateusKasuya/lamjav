{{
  config(
    description='NBA prop player stats vs line per game',
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
)

SELECT
    gps.player_id,
    gps.game_date,
    gps.game_id,
    gps.stat_type,
    gps.stat_value,
    o.line,
    g.is_b2b_game,
    CASE
        WHEN o.line IS null THEN null
        WHEN gps.stat_value >= line THEN 'over'
        WHEN gps.stat_value < line THEN 'under'
    END AS stat_vs_line,
    CASE
        WHEN gps.team_id = g.home_team_id THEN g.visitor_team_abbreviation
        WHEN gps.team_id = g.visitor_team_id THEN '@' || g.home_team_abbreviation
    END AS played_against,
    CASE
        WHEN gps.team_id = g.home_team_id THEN 'Casa'
        WHEN gps.team_id = g.visitor_team_id THEN 'Fora'
    END AS home_away
FROM
    {{ ref('int_game_player_stats') }} AS gps
LEFT JOIN {{ ref('stg_games') }} AS g ON gps.game_id = g.game_id AND g.game_date < CURRENT_DATE()
LEFT JOIN {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para ON gps.player_id = de_para.nba_player_id
LEFT JOIN historical_event_odds AS o ON de_para.odds_player_name = o.player_name AND gps.stat_type = o.market_key
ORDER BY
    player_id,
    game_id
