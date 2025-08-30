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
        dpp.injury_player_name,  -- Adicionar o nome do injury report
        coalesce(sl.points_rank = 1 AND ir.current_status IS NOT null, false) AS leader_points_out_injury,
        coalesce(sl.points_rank > 1 AND ir.current_status IS null, false) AS second_leader_points_available,
        coalesce(sl.rebounds_rank = 1 AND ir.current_status IS NOT null, false) AS leader_rebounds_out_injury,
        coalesce(sl.rebounds_rank > 1 AND ir.current_status IS null, false) AS second_leader_rebounds_available,
        coalesce(sl.assists_rank = 1 AND ir.current_status IS NOT null, false) AS leader_assists_out_injury,
        coalesce(sl.assists_rank > 1 AND ir.current_status IS null, false) AS second_leader_assists_available
    FROM {{ ref('int_stats_leaders') }} AS sl
    LEFT JOIN {{ source('bi_dev', 'de_para_players') }} AS dpp
        ON sl.player_id = dpp.nba_player_id
    LEFT JOIN {{ ref('stg_injury_report') }} AS ir
        ON dpp.injury_player_name = ir.player_name
),

-- Performance comparison data
player_performance_comparison AS (
    SELECT
        ppc.team_id,
        ppc.injured_player_name,
        ppc.player_id,
        ppc.full_name,
        ppc.avg_points_when_injured_out,
        ppc.avg_rebounds_when_injured_out,
        ppc.avg_assists_when_injured_out,
        ppc.avg_points_normal,
        ppc.avg_rebounds_normal,
        ppc.avg_assists_normal
    FROM {{ ref('int_player_performance_comparison') }} AS ppc
),

injury_insights AS (
    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'points' AS category,
        -- Performance comparison stats
        coalesce(ppc.avg_points_when_injured_out, 0) AS stats_when_target_player_out,
        coalesce(ppc.avg_points_normal, 0) AS stats_when_target_player_in
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_points_available = true
    LEFT JOIN player_performance_comparison AS ppc
        ON
            one.team_id = ppc.team_id
            AND ppc.injured_player_name = one.injury_player_name  -- Usar o nome correto do injury report
            AND two.full_name = ppc.full_name
    WHERE one.leader_points_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.points_rank) = 1

    UNION ALL

    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'rebounds' AS category,
        -- Performance comparison stats
        coalesce(ppc.avg_rebounds_when_injured_out, 0) AS stats_when_target_player_out,
        coalesce(ppc.avg_rebounds_normal, 0) AS stats_when_target_player_in
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_rebounds_available = true
    LEFT JOIN player_performance_comparison AS ppc
        ON
            one.team_id = ppc.team_id
            AND ppc.injured_player_name = one.injury_player_name  -- Usar o nome correto do injury report
            AND two.full_name = ppc.full_name
    WHERE one.leader_rebounds_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.rebounds_rank) = 1

    UNION ALL

    SELECT
        one.team_name,
        one.full_name AS first_player_rank_out_injury,
        two.full_name AS next_player_rank_available,
        'assists' AS category,
        -- Performance comparison stats
        coalesce(ppc.avg_assists_when_injured_out, 0) AS stats_when_target_player_out,
        coalesce(ppc.avg_assists_normal, 0) AS stats_when_target_player_in
    FROM team_leaders_first_two AS one
    LEFT JOIN team_leaders_first_two AS two
        ON
            one.team_id = two.team_id
            AND two.second_leader_assists_available = true
    LEFT JOIN player_performance_comparison AS ppc
        ON
            one.team_id = ppc.team_id
            AND ppc.injured_player_name = one.injury_player_name  -- Usar o nome correto do injury report
            AND two.full_name = ppc.full_name
    WHERE one.leader_assists_out_injury = true
    QUALIFY row_number() OVER (PARTITION BY one.team_id ORDER BY two.assists_rank) = 1
)

SELECT
    *,
    current_timestamp() AS loaded_at
FROM injury_insights
ORDER BY team_name
