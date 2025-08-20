{{ config(
    materialized='view',
    description='Staging table for NBA games from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_games') }}
),

cleaned_data AS (
    SELECT
        id AS game_id,
        --season,
        date AS game_date,

        -- Game state information
        postseason AS is_postseason,

        -- Home team information
        home_team.id AS home_team_id,

        -- Visitor team information
        visitor_team.id AS visitor_team_id,

        -- Scores (convert from FLOAT to INTEGER)
        CAST(home_team_score AS INTEGER) AS home_team_score,
        CAST(visitor_team_score AS INTEGER) AS visitor_team_score,

        -- Calculated fields

        CASE
            WHEN home_team_score IS NOT null AND visitor_team_score IS NOT null
                THEN
                    CASE
                        WHEN home_team_score > visitor_team_score THEN 'HOME'
                        WHEN visitor_team_score > home_team_score THEN 'VISITOR'
                        ELSE 'TIE'
                    END
        END AS winner,

        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
)

SELECT * FROM cleaned_data
