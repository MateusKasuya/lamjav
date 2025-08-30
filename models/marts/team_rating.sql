{{ config(
    materialized='table',
    schema='bi'
) }}

WITH player_stats AS (
    SELECT
        t.team_full_name,
        t.team_abbreviation,
        sum(offensive_rating_points) / sum(possessions) AS team_offensive_rating,
        sum(defensive_rating_points) / sum(possessions) AS team_defensive_rating,
        current_timestamp() AS loaded_at
    FROM {{ ref('stg_season_averages_general_advanced') }} AS sga
    INNER JOIN {{ ref('stg_active_players') }} AS ap ON sga.player_id = ap.player_id
    INNER JOIN {{ ref('stg_teams') }} AS t ON ap.team_id = t.team_id
    GROUP BY t.team_full_name, t.team_abbreviation
)

SELECT * FROM player_stats
