{{
  config(
    materialized='table',
    schema='bi',
    description='NBA injury insights for team leaders analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH team_leaders_first_two AS (
    SELECT
        sl.player_id,
        sl.full_name,
        sl.team_id,
        sl.team_name,
        sl.avg_points,
        sl.avg_total_rebounds,
        sl.avg_assists,
        sl.points_rank,
        sl.rebounds_rank,
        sl.assists_rank,
        ir.current_status,
        coalesce(sl.points_rank = 1 AND ir.current_status IS NOT null, FALSE) AS leader_points_out_injury,
        coalesce(sl.points_rank > 1 AND ir.current_status IS null, FALSE) AS second_leader_points_available,
        coalesce(sl.rebounds_rank = 1 AND ir.current_status IS NOT null, FALSE) AS leader_rebounds_out_injury,
        coalesce(sl.rebounds_rank > 1 AND ir.current_status IS null, FALSE) AS second_leader_rebounds_available,
        coalesce(sl.assists_rank = 1 AND ir.current_status IS NOT null, FALSE) AS leader_assists_out_injury,
        coalesce(sl.assists_rank > 1 AND ir.current_status IS null, FALSE) AS second_leader_assists_available
    FROM {{ ref('int_stats_leaders') }} AS sl
    LEFT JOIN {{ source('bi_dev', 'de_para_players') }} AS dpp
        ON sl.player_id = dpp.nba_player_id
    LEFT JOIN {{ ref('stg_injury_report') }} AS ir
        ON dpp.injury_player_name = ir.player_name
),

injury_insights AS (
    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'points' AS category
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_points_available = true
    WHERE one.leader_points_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.points_rank) = 1

    UNION ALL

    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'rebounds' AS category
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_rebounds_available = true
    WHERE one.leader_rebounds_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.rebounds_rank) = 1

    UNION ALL

    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'assists' AS category
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_assists_available = true
    WHERE one.leader_assists_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.assists_rank) = 1
)

SELECT * FROM injury_insights
ORDER BY team_name
