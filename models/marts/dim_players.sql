{{
  config(
    materialized='table',
    schema='bi',
    description='NBA players for analysis',
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
        minutes > 0
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
    p.id,
    p.name,
    p.position,
    p.team_id,
    p.team_name,
    p.team_abbreviation,
    s.age,
    s.games_played,
    s.minutes,
    g.last_game_text,
    CURRENT_TIMESTAMP() AS loaded_at
FROM
    {{ ref('stg_active_players') }} AS p
LEFT JOIN
    {{ ref('stg_season_averages_general_base') }} AS s
    ON
        p.id = s.player_id
LEFT JOIN game_player_stats AS g ON p.id = g.player_id
