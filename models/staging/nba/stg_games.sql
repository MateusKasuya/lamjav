{{ config(
    materialized='view',
    description='Staging table for NBA games from NDJSON external table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('nba', 'raw_games') }}
),

cleaned_data AS (
    SELECT
        id,
        --season,
        --date AS game_date,
        --datetime AS game_datetime,
        -- Convert datetime to Brasília timezone (UTC-3)
        --postseason AS is_postseason,

        -- Game state information
        home_team.id AS home_team_id,
        home_team.abbreviation AS home_team_abbreviation,

        -- Home team information
        visitor_team.id AS visitor_team_id,
        visitor_team.abbreviation AS visitor_team_abbreviation,

        -- Visitor team information
        CASE
            WHEN datetime IS NOT null
                THEN DATETIME_ADD(CAST(datetime AS DATETIME), INTERVAL -3 HOUR)
        END AS game_datetime_brasilia,

        -- Scores (convert from FLOAT to INTEGER)
        --CAST(home_team_score AS INTEGER) AS home_team_score,
        --CAST(visitor_team_score AS INTEGER) AS visitor_team_score,

        -- Calculated fields

        /*CASE
            WHEN home_team_score IS NOT null AND visitor_team_score IS NOT null
                THEN
                    CASE
                        WHEN home_team_score > visitor_team_score THEN 'HOME'
                        WHEN visitor_team_score > home_team_score THEN 'VISITOR'
                        ELSE 'TIE'
                    END
        END AS winner,*/

        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE date < '2025-04-07'
)

SELECT * FROM cleaned_data
