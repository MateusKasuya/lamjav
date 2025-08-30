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
        ON gps.game_id = g.game_id AND g.game_date <= '2025-04-07'
    INNER JOIN {{ ref('stg_active_players') }} AS ap
        ON gps.player_id = ap.player_id
    INNER JOIN {{ ref('stg_teams') }} AS t
        ON ap.team_id = t.team_id
    WHERE gps.minutes_played > 0
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
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY avg_assists DESC) AS assists_rank,
        AVG(avg_points) OVER (PARTITION BY team_id) AS avg_team_points,
        AVG(avg_total_rebounds) OVER (PARTITION BY team_id) AS avg_team_rebounds,
        AVG(avg_assists) OVER (PARTITION BY team_id) AS avg_team_assists,

        -- Desvios padrão em relação à média do time
        STDDEV(avg_points) OVER (PARTITION BY team_id) AS stddev_team_points,
        STDDEV(avg_total_rebounds) OVER (PARTITION BY team_id) AS stddev_team_rebounds,
        STDDEV(avg_assists) OVER (PARTITION BY team_id) AS stddev_team_assists

    FROM player_team_averages
),

zscore_stats AS (
    SELECT
        *,
        (avg_points - avg_team_points) / stddev_team_points AS points_zscore,
        (avg_total_rebounds - avg_team_rebounds) / stddev_team_rebounds AS rebounds_zscore,
        (avg_assists - avg_team_assists) / stddev_team_assists AS assists_zscore
    FROM team_leaders
)

SELECT
    *,
    CASE
        WHEN points_zscore > 1.67 THEN 3
        WHEN points_zscore >= 1 THEN 2
        WHEN points_zscore >= 0 THEN 1
        ELSE 0
    END AS points_rating,
    CASE
        WHEN rebounds_zscore > 1.7 THEN 3
        WHEN rebounds_zscore >= 0 THEN 1
        ELSE 0
    END AS rebounds_rating,
    CASE
        WHEN assists_zscore > 1.7 THEN 3
        WHEN assists_zscore >= 0 THEN 1
        ELSE 0
    END AS assists_rating,
    CURRENT_TIMESTAMP() AS loaded_at
FROM zscore_stats
