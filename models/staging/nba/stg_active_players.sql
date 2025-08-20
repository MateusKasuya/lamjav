{{ config(
    materialized='view',
    description='Staging table for NBA active players from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_active_players') }}
),

cleaned_data AS (
    SELECT
        id AS player_id,
        team.id AS team_id,
        TRIM(first_name || ' ' || last_name) AS full_name,
        TRIM(last_name || ', ' || first_name) AS last_name_first,
        TRIM(position) AS position,
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM cleaned_data
