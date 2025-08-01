{{ config(
    materialized='view',
    description='Staging table for NBA team standings from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_team_standings') }}
),

cleaned_data AS (
    SELECT
        team.id AS team_id,
        season,
        CAST(wins AS INT64) AS wins,
        CAST(losses AS INT64) AS losses,
        CAST(ROUND(wins / (wins + losses) * 100) AS INT64) AS win_percentage,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM cleaned_data
