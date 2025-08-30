{{
  config(
    materialized='view',
    schema='nba',
    description='Player performance comparison when target players are injured'
  )
}}

WITH player_injury_periods AS (
    -- Identifica períodos quando jogadores estavam lesionados
    SELECT DISTINCT
        dpp.nba_player_id AS injured_player_id,
        dpp.injury_player_name AS injured_player_name,
        gps.team_id,
        g.game_date,
        g.game_id
    FROM {{ ref('stg_game_player_stats') }} AS gps
    INNER JOIN {{ ref('stg_games') }} AS g
        ON gps.game_id = g.game_id
    INNER JOIN {{ source('bi_dev', 'de_para_players') }} AS dpp
        ON gps.player_id = dpp.nba_player_id
    INNER JOIN {{ ref('stg_injury_report') }} AS ir
        ON dpp.nba_player_name = ir.player_name
    WHERE gps.minutes_played = 0  -- Jogador não jogou (lesionado)
),

player_stats_when_injured_out AS (
    -- Calcula estatísticas de outros jogadores quando o lesionado não joga
    SELECT
        pip.team_id,
        pip.game_id,
        pip.injured_player_name,
        gps.player_id,
        ap.full_name,
        AVG(gps.points) AS avg_points_when_injured_out,
        AVG(gps.total_rebounds) AS avg_rebounds_when_injured_out,
        AVG(gps.assists) AS avg_assists_when_injured_out,
        COUNT(DISTINCT gps.game_id) AS games_analyzed
    FROM player_injury_periods AS pip
    INNER JOIN {{ ref('stg_game_player_stats') }} AS gps
        ON
            pip.team_id = gps.team_id
            AND pip.game_id = gps.game_id
            AND gps.minutes_played > 0  -- Jogador jogou
            AND pip.injured_player_id != gps.player_id  -- Excluir o jogador lesionado
    INNER JOIN {{ ref('stg_active_players') }} AS ap
        ON gps.player_id = ap.player_id
    GROUP BY
        pip.team_id,
        pip.game_id,
        pip.injured_player_name,
        gps.player_id,
        ap.full_name
),

player_stats_normal AS (
    -- Estatísticas normais dos jogadores (quando não há lesões específicas)
    SELECT
        gps.player_id,
        ap.full_name,
        gps.team_id,
        AVG(gps.points) AS avg_points_normal,
        AVG(gps.total_rebounds) AS avg_rebounds_normal,
        AVG(gps.assists) AS avg_assists_normal,
        COUNT(DISTINCT gps.game_id) AS total_games
    FROM {{ ref('stg_game_player_stats') }} AS gps
    INNER JOIN {{ ref('stg_games') }} AS g
        ON gps.game_id = g.game_id
    INNER JOIN {{ ref('stg_active_players') }} AS ap
        ON gps.player_id = ap.player_id
    WHERE gps.minutes_played > 0
    GROUP BY gps.player_id, ap.full_name, gps.team_id
)

SELECT
    psio.team_id,
    psio.injured_player_name,
    psio.player_id,
    psio.full_name,
    psio.avg_points_when_injured_out,
    psio.avg_rebounds_when_injured_out,
    psio.avg_assists_when_injured_out,
    psn.avg_points_normal,
    psn.avg_rebounds_normal,
    psn.avg_assists_normal,
    psio.games_analyzed,
    psn.total_games,
    CURRENT_TIMESTAMP() AS loaded_at
FROM player_stats_when_injured_out AS psio
INNER JOIN player_stats_normal AS psn
    ON
        psio.player_id = psn.player_id
        AND psio.team_id = psn.team_id
