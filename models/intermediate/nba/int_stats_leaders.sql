{{
  config(
    materialized='view',
    description='Intermediate table for NBA team leaders by category (points, rebounds, assists)',
    labels={'domain': 'nba', 'category': 'analytics'}
  )
}}

WITH player_team_averages AS (
    SELECT
        ap.player_id,
        ap.full_name,
        t.team_id,
        t.team_full_name AS team_name,
        AVG(gps.points) AS avg_points,
        AVG(gps.total_rebounds) AS avg_total_rebounds,
        AVG(gps.assists) AS avg_assists
    FROM {{ ref('stg_game_player_stats') }} AS gps
    INNER JOIN {{ ref('stg_games') }} AS g
        ON gps.game_id = g.game_id
    INNER JOIN {{ ref('stg_active_players') }} AS ap
        ON gps.player_id = ap.player_id
    INNER JOIN {{ ref('stg_teams') }} AS t
        ON ap.team_id = t.team_id
    WHERE
        g.game_date <= '2025-04-07'
    GROUP BY
        ap.player_id,
        ap.full_name,
        t.team_id,
        t.team_full_name
),

team_leaders AS (
    SELECT
        player_id,
        full_name,
        team_id,
        team_name,
        avg_points,
        avg_total_rebounds,
        avg_assists,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY avg_points DESC) AS points_rank,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY avg_total_rebounds DESC) AS rebounds_rank,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY avg_assists DESC) AS assists_rank
    FROM player_team_averages
)

SELECT * FROM team_leaders
ORDER BY team_id, points_rank
