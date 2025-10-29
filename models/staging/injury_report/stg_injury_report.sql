{{ config(
    description='Staging table for NBA injury report data from raw table'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('injury_report', 'raw_injury_report') }}
),

cleaned_data AS (
    SELECT
        -- Player information
        current_status,
        TRIM(player_name) AS player_name,

        -- Injury Report Date (extract from source_file: injury_report_2025-04-07_06PM.pdf)
        PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(source_file, r'injury_report_(\d{4}-\d{2}-\d{2})_'))
            AS injury_report_date,

        -- Injury Report Time (extract from source_file: injury_report_2025-04-07_06PM.pdf)
        REGEXP_EXTRACT(source_file, r'_(\d{2}[AP]M)\.pdf$') AS injury_report_time,

        -- Additional metadata
        CURRENT_TIMESTAMP() AS loaded_at
    FROM source_data
    WHERE current_status IN ('Doubtful', 'Questionable', 'Out')
)

SELECT * FROM cleaned_data
QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY injury_report_date DESC, loaded_at DESC) = 1
