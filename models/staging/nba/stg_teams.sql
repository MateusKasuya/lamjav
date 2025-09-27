{{ config(
    materialized='view',
    description='Staging table for NBA teams from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_teams') }}
),

cleaned_data AS (
    SELECT
        id AS team_id,
        TRIM(full_name) AS team_name,
        UPPER(TRIM(abbreviation)) AS team_abbreviation,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM cleaned_data
