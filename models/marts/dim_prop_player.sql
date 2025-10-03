{{
  config(
    materialized='table',
    schema='bi',
    description='NBA prop player analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH
-- Latest odds data
latest_odds AS (
    SELECT
        player_name,
        market_key,
        line
    FROM {{ ref('stg_historical_event_odds') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_name, market_key
        ORDER BY snapshot_timestamp DESC
    ) = 1
),

-- Games where players didn't play (injury periods)
-- Note: This identifies games where players had 0 minutes, which could indicate injury
injury_games AS (
    SELECT DISTINCT
        player_id,
        team_id,
        game_id
    FROM {{ ref('stg_game_player_stats') }}
    WHERE minutes = 0
),

-- Alternative: Use injury report data to identify injured players
injured_players_from_report AS (
    SELECT DISTINCT
        dpp.nba_player_id AS player_id,
        ap.team_id,
        ir.current_status
    FROM {{ source('bi_dev', 'de_para_nba_injury_players') }} AS dpp
    INNER JOIN {{ ref('stg_injury_report') }} AS ir
        ON dpp.injury_player_name = ir.player_name
    INNER JOIN {{ ref('stg_active_players') }} AS ap
        ON dpp.nba_player_id = ap.id
    WHERE ir.current_status IN ('Out', 'Doubtful', 'Questionable')
),

-- Performance of backup players when specific leaders are injured
backup_performance_when_leader_injured AS (
    SELECT
        ipr.player_id AS injured_leader_id,
        ipr.team_id,
        gps.stat_type,
        backup_player.player_id AS backup_player_id,
        AVG(backup_player.stat_value) AS backup_stats_when_leader_out
    FROM injury_games AS ipr
    INNER JOIN {{ ref('int_game_player_stats') }} AS gps
        ON
            ipr.player_id = gps.player_id
            AND ipr.team_id = gps.team_id
    INNER JOIN {{ ref('int_game_player_stats') }} AS backup_player
        ON
            ipr.team_id = backup_player.team_id
            AND gps.game_id = backup_player.game_id
            AND gps.stat_type = backup_player.stat_type
            AND ipr.player_id != backup_player.player_id
    GROUP BY
        ipr.player_id,
        ipr.team_id,
        gps.stat_type,
        backup_player.player_id
),

-- Normal performance of backup players (all games, not just when no leader is injured)
backup_performance_normal AS (
    SELECT
        gps.player_id,
        gps.team_id,
        gps.stat_type,
        AVG(gps.stat_value) AS backup_stats_normal
    FROM {{ ref('int_game_player_stats') }} AS gps
    -- Note: int_game_player_stats already filters for minutes > 0 in its base_data CTE
    GROUP BY gps.player_id, gps.team_id, gps.stat_type
),

-- Base player statistics with odds and rankings
player_base_stats AS (
    SELECT
        p.id AS player_id,
        p.team_id,
        s.stat_type,
        s.stat_value,
        o.line,
        CASE
            WHEN o.market_key IN ('player_double_double', 'player_triple_double') THEN null
            ELSE s.stat_value - o.line
        END AS delta,
        ROW_NUMBER() OVER (
            PARTITION BY p.team_id, s.stat_type
            ORDER BY s.stat_value DESC
        ) AS stat_rank,
        AVG(s.stat_value) OVER (PARTITION BY p.team_id, s.stat_type) AS team_avg_stat,
        STDDEV(s.stat_value) OVER (PARTITION BY p.team_id, s.stat_type) AS team_stddev_stat
    FROM {{ ref('stg_active_players') }} AS p
    LEFT JOIN {{ ref('int_season_averages_general_base') }} AS s
        ON p.id = s.player_id
    LEFT JOIN {{ source('bi_dev', 'de_para_nba_odds_players') }} AS de_para
        ON p.id = de_para.nba_player_id
    LEFT JOIN latest_odds AS o
        ON
            de_para.odds_player_name = o.player_name
            AND s.stat_type = o.market_key
),

-- Calculate z-scores and ratings
player_ratings AS (
    SELECT
        *,
        CASE
            WHEN team_stddev_stat IS null OR team_stddev_stat = 0 THEN 0
            ELSE (stat_value - team_avg_stat) / team_stddev_stat
        END AS zscore,
        CASE
            WHEN (stat_value - team_avg_stat) / NULLIF(team_stddev_stat, 0) > 1.67 THEN 3
            WHEN (stat_value - team_avg_stat) / NULLIF(team_stddev_stat, 0) >= 1 THEN 2
            WHEN (stat_value - team_avg_stat) / NULLIF(team_stddev_stat, 0) >= 0 THEN 1
            ELSE 0
        END AS rating_stars
    FROM player_base_stats
),

-- Add injury status information
players_with_injury_status AS (
    SELECT
        pr.*,
        ir.current_status,
        COALESCE(pr.stat_rank = 1 AND ir.current_status IS NOT null, FALSE) AS is_leader_with_injury,
        COALESCE(pr.stat_rank > 1, FALSE) AS is_available_backup
    FROM player_ratings AS pr
    LEFT JOIN {{ source('bi_dev', 'de_para_nba_injury_players') }} AS dpp
        ON pr.player_id = dpp.nba_player_id
    LEFT JOIN {{ ref('stg_injury_report') }} AS ir
        ON dpp.injury_player_name = ir.player_name
),

-- Identify next available players for each stat (always show next best player)
next_available_players AS (
    SELECT
        leader.team_id,
        leader.stat_type,
        leader.player_id AS current_leader_id,
        backup.player_id AS next_available_player_id,
        p.name AS next_available_player_name,
        COALESCE(bpwi.backup_stats_when_leader_out, 0) AS next_player_stats_when_leader_out,
        COALESCE(bpn.backup_stats_normal, 0) AS next_player_stats_normal
    FROM players_with_injury_status AS leader
    LEFT JOIN players_with_injury_status AS backup
        ON
            leader.team_id = backup.team_id
            AND leader.stat_type = backup.stat_type
            AND backup.is_available_backup = true
    LEFT JOIN {{ ref('stg_active_players') }} AS p
        ON backup.player_id = p.id
    LEFT JOIN backup_performance_when_leader_injured AS bpwi
        ON
            leader.player_id = bpwi.injured_leader_id
            AND leader.team_id = bpwi.team_id
            AND leader.stat_type = bpwi.stat_type
            AND backup.player_id = bpwi.backup_player_id
    LEFT JOIN backup_performance_normal AS bpn
        ON
            backup.team_id = bpn.team_id
            AND backup.player_id = bpn.player_id
            AND backup.stat_type = bpn.stat_type
    WHERE leader.stat_rank = 1  -- Always show next player for the current leader
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY leader.team_id, leader.stat_type
        ORDER BY backup.stat_rank
    ) = 1
)

-- Final result combining all data
-- Shows next available player info with conditional logic for injury-related stats
SELECT
    player_id,
    team_id,
    stat_type,
    stat_value,
    line,
    delta,
    rating_stars,
    next_available_player_name,
    next_player_stats_when_leader_out,
    next_player_stats_normal,
    CURRENT_TIMESTAMP() AS loaded_at
FROM (
    SELECT
        pwis.*,
        -- Always show next available player name and normal stats
        nap.next_available_player_name,
        nap.next_player_stats_normal,
        -- Only show leader_out stats when current player is a leader with injury
        CASE
            WHEN pwis.is_leader_with_injury = true THEN nap.next_player_stats_when_leader_out
        END AS next_player_stats_when_leader_out
    FROM players_with_injury_status AS pwis
    LEFT JOIN next_available_players AS nap
        ON
            pwis.team_id = nap.team_id
            AND pwis.stat_type = nap.stat_type
)
ORDER BY player_id, stat_type
