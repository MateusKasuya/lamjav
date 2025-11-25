{{ config(
    description='Staging table for NBA games with B2B flag from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_games') }}
),

cleaned_data AS (
    SELECT
        id AS game_id,
        --season,
        date AS game_date,
        --status AS game_status,

        --datetime AS game_datetime,
        -- Convert datetime to Brasília timezone (UTC-3)
        --postseason AS is_postseason,

        -- Game state information
        home_team.id AS home_team_id,
        home_team.full_name AS home_team_name,
        home_team.abbreviation AS home_team_abbreviation,
        home_team_score,

        -- Home team information
        visitor_team.id AS visitor_team_id,
        visitor_team.full_name AS visitor_team_name,
        visitor_team.abbreviation AS visitor_team_abbreviation,
        visitor_team_score,

        -- Visitor team information
        --CASE
        --WHEN datetime IS NOT null
        --THEN DATETIME_ADD(CAST(datetime AS DATETIME), INTERVAL -3 HOUR)
        --END AS game_datetime_brasilia,

        -- Scores (convert from FLOAT to INTEGER)
        --CAST(home_team_score AS INTEGER) AS home_team_score,
        --CAST(visitor_team_score AS INTEGER) AS visitor_team_score,

        -- Calculated fields

        CASE
            WHEN home_team_score > visitor_team_score THEN home_team.id
            WHEN visitor_team_score > home_team_score THEN visitor_team.id
        END AS winner_team_id,

        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    --WHERE date < '2025-04-07'
)

SELECT * FROM cleaned_data
