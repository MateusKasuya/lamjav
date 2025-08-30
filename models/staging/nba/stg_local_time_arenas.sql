{{
  config(
    materialized='view',
    schema='nba'
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('raw_local_time_arenas') }}
),

renamed AS (
    SELECT
        -- Arena information
        abbreviation AS team_abbreviation,

        -- Time information
        horario_local_fixo AS local_injury_report_time,
        horario_brasilia AS brasilia_injury_report_time,

        -- Metadata
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM source
)

SELECT * FROM renamed
