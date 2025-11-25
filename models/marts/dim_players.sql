{{
  config(
    description='NBA players for analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH dim_players AS (

    SELECT
        p.player_id,
        p.player_name,
        p.position,
        p.team_id,
        p.team_name,
        p.team_abbreviation,
        s.age,
        g.last_game_text,
        ir.current_status,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM
        {{ ref('stg_active_players') }} AS p
    LEFT JOIN
        {{ ref('stg_season_averages_general_base') }} AS s
        ON p.player_id = s.player_id
    LEFT JOIN {{ ref('int_game_player_stats_last_game_text') }} AS g ON p.player_id = g.player_id
    LEFT JOIN {{ source('bi_dev', 'de_para_nba_injury_players') }} AS de_para
        ON p.player_id = de_para.nba_player_id
        AND de_para.similarity_score > 70
    LEFT JOIN {{ ref('stg_injury_report') }} AS ir
        ON de_para.injury_player_name = ir.player_name
)

SELECT * FROM dim_players
