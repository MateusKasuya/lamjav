{{ config(
    description='Staging table for NBA team standings from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_team_standings') }}
),

cleaned_data AS (
    SELECT
        team.id AS team_id,
        season,
        conference_rank,
        CAST(wins AS INT64) AS wins,
        CAST(losses AS INT64) AS losses,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM cleaned_data
