{{
  config(
    materialized='table',
    schema='bi',
    description='NBA teams for analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

SELECT
    team_id,
    team_name,
    team_abbreviation,
    CURRENT_TIMESTAMP() AS loaded_at
FROM
    {{ ref('stg_teams') }}
