{{
  config(
    materialized='table',
    schema='bi',
    description='NBA prop player analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH last_games AS (
    SELECT
        player_id,
        MAX(game_date) AS last_game_date
    FROM
        {{ ref('stg_game_player_stats') }}
    WHERE
        minutes_played > 0
    GROUP BY
        player_id
),

game_player_stats AS (
    SELECT
        player_id,
        CONCAT(
            'Ultimo Jogo: ',
            CAST(DATE_DIFF(DATE '2025-04-07', last_game_date, DAY) AS STRING),
            ' ',
            CASE
                WHEN DATE_DIFF(DATE '2025-04-07', last_game_date, DAY) = 1 THEN 'dia atras'
                ELSE 'dias atras'
            END
        ) AS last_game_text
    FROM
        last_games
)

SELECT
    -- Identificação
    p.full_name,
    p.team_full_name,
    p.position,
    s.player_age,

    -- Geral
    s.games_played,
    s.minutes,
    s.wins,
    s.losses,
    g.last_game_text,

    -- Points
    s.points,
    o.points_over_line,
    o.points_over_price,
    o.points_under_line,
    o.points_under_price,

    -- Threes
    s.three_pointers_made,
    s.three_pointers_attempted,
    o.threes_over_line,
    o.threes_over_price,
    o.threes_under_line,
    o.threes_under_price,

    -- Rebounds
    s.rebounds,
    o.rebounds_over_line,
    o.rebounds_over_price,
    o.rebounds_under_line,
    o.rebounds_under_price,

    -- Assists
    s.assists,
    o.assists_over_line,
    o.assists_over_price,
    o.assists_under_line,
    o.assists_under_price,

    -- Steals
    s.steals,
    o.steals_over_line,
    o.steals_over_price,
    o.steals_under_line,
    o.steals_under_price,

    -- Blocks
    s.blocks,
    o.blocks_over_line,
    o.blocks_over_price,
    o.blocks_under_line,
    o.blocks_under_price,

    -- Blocks + Steals
    s.blocks_steals,
    o.blocks_steals_over_line,
    o.blocks_steals_over_price,
    o.blocks_steals_under_line,
    o.blocks_steals_under_price,

    -- Turnovers
    s.turnovers,
    o.turnovers_over_line,
    o.turnovers_over_price,
    o.turnovers_under_line,
    o.turnovers_under_price,

    -- Combinados
    s.rebounds_assists,
    o.ra_over_line,
    o.ra_over_price,
    o.ra_under_line,
    o.ra_under_price,

    s.points_assists,
    o.pa_over_line,
    o.pa_over_price,
    o.pa_under_line,
    o.pa_under_price,

    s.points_rebounds,
    o.pr_over_line,
    o.pr_over_price,
    o.pr_under_line,
    o.pr_under_price,

    s.points_rebounds_assists,
    o.pra_over_line,
    o.pra_over_price,
    o.pra_under_line,
    o.pra_under_price,

    -- Double Double
    s.double_doubles,
    o.double_double_yes_price,
    o.double_double_no_price,

    -- Triple Double
    s.triple_doubles,
    o.triple_double_yes_price,
    o.triple_double_no_price

FROM
    {{ ref('stg_active_players') }} AS p
LEFT JOIN
    {{ ref('stg_season_averages_general_base') }} AS s
    ON p.player_id = s.player_id
LEFT JOIN
    {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para
    ON p.player_id = de_para.nba_player_id
LEFT JOIN
    {{ ref('int_odds_pivoted') }} AS o
    ON de_para.odds_player_name = o.player_name
LEFT JOIN
    game_player_stats AS g
    ON p.player_id = g.player_id
ORDER BY
    p.team_full_name,
    p.full_name
