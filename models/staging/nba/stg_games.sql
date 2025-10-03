{{ config(
    materialized='view',
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
        --datetime AS game_datetime,
        -- Convert datetime to Brasília timezone (UTC-3)
        --postseason AS is_postseason,

        -- Game state information
        home_team.id AS home_team_id,
        home_team.full_name AS home_team_name,
        home_team.abbreviation AS home_team_abbreviation,

        -- Home team information
        visitor_team.id AS visitor_team_id,
        visitor_team.full_name AS visitor_team_name,
        visitor_team.abbreviation AS visitor_team_abbreviation,

        -- Visitor team information
        --CASE
        --WHEN datetime IS NOT null
        --THEN DATETIME_ADD(CAST(datetime AS DATETIME), INTERVAL -3 HOUR)
        --END AS game_datetime_brasilia,

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
    --WHERE date < '2025-04-07'
),

-- Create all team games to identify B2B patterns and next games
all_team_games AS (
    -- Home team games
    SELECT
        game_id,
        home_team_id AS team_id,
        home_team_abbreviation AS team_abbreviation,
        home_team_name AS team_name,
        game_date
    FROM cleaned_data

    UNION ALL

    -- Visitor team games
    SELECT
        game_id,
        visitor_team_id AS team_id,
        visitor_team_abbreviation AS team_abbreviation,
        visitor_team_name AS team_name,
        game_date
    FROM cleaned_data
),

-- Identify consecutive games for each team (only for games before 2025-04-07 for B2B analysis)
consecutive_games AS (
    SELECT
        game_id,
        team_id,
        team_abbreviation,
        team_name,
        game_date,
        LAG(game_id) OVER (PARTITION BY team_id ORDER BY game_date, game_id) AS previous_game_id,
        LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date, game_id) AS previous_game_date,
        DATE_DIFF(game_date, LAG(game_date) OVER (PARTITION BY team_id ORDER BY game_date, game_id), DAY)
            AS days_between_games
    FROM all_team_games
    WHERE game_date < '2025-04-07'
),

-- Identify B2B games
b2b_games AS (
    SELECT DISTINCT
        game_id,
        true AS is_b2b_game
    FROM consecutive_games
    WHERE
        days_between_games = 1
        AND previous_game_date IS NOT null  -- Ensure there's a previous game
),

-- Identify next game for each team (starting from 2025-04-07)
next_games AS (
    SELECT DISTINCT
        game_id,
        true AS is_next_game
    FROM all_team_games
    WHERE
        game_date >= '2025-04-07'
        AND game_id IN (
            -- Get the first game for each team on or after 2025-04-07
            SELECT
                FIRST_VALUE(game_id) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date ASC, game_id ASC
                ) AS first_game_id
            FROM all_team_games
            WHERE game_date >= '2025-04-07'
        )
),

-- Final result with B2B and next game flags
final_result AS (
    SELECT
        cd.*,
        COALESCE(bb.is_b2b_game, false) AS is_b2b_game,
        COALESCE(ng.is_next_game, false) AS is_next_game
    FROM cleaned_data AS cd
    LEFT JOIN b2b_games AS bb ON cd.game_id = bb.game_id
    LEFT JOIN next_games AS ng ON cd.game_id = ng.game_id
)

SELECT * FROM final_result
