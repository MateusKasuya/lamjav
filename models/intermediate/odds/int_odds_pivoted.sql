{{ config(
    materialized='view',
    description='Intermediate model with pivoted odds data - transforms long format to wide format for analysis'
) }}

WITH base_odds AS (
    SELECT * FROM {{ ref('stg_historical_event_odds') }}
),

-- Create a proper tabular pivot using conditional aggregation
pivoted_odds AS (
    SELECT
        -- Primary keys for grouping
        event_id,
        bookmaker_key,
        player_name,

        -- Event details
        commence_time,

        -- Timestamps
        snapshot_timestamp,
        extraction_timestamp,

        -- Pivot all combinations dynamically using MAX + CASE
        -- Points
        MAX(CASE WHEN market_key = 'player_points' AND outcome_name = 'Over' THEN point END) AS points_over_line,
        MAX(CASE WHEN market_key = 'player_points' AND outcome_name = 'Over' THEN price END) AS points_over_price,
        MAX(CASE WHEN market_key = 'player_points' AND outcome_name = 'Under' THEN point END) AS points_under_line,
        MAX(CASE WHEN market_key = 'player_points' AND outcome_name = 'Under' THEN price END) AS points_under_price,

        -- Rebounds
        MAX(CASE WHEN market_key = 'player_rebounds' AND outcome_name = 'Over' THEN point END) AS rebounds_over_line,
        MAX(CASE WHEN market_key = 'player_rebounds' AND outcome_name = 'Over' THEN price END) AS rebounds_over_price,
        MAX(CASE WHEN market_key = 'player_rebounds' AND outcome_name = 'Under' THEN point END) AS rebounds_under_line,
        MAX(CASE WHEN market_key = 'player_rebounds' AND outcome_name = 'Under' THEN price END) AS rebounds_under_price,

        -- Assists
        MAX(CASE WHEN market_key = 'player_assists' AND outcome_name = 'Over' THEN point END) AS assists_over_line,
        MAX(CASE WHEN market_key = 'player_assists' AND outcome_name = 'Over' THEN price END) AS assists_over_price,
        MAX(CASE WHEN market_key = 'player_assists' AND outcome_name = 'Under' THEN point END) AS assists_under_line,
        MAX(CASE WHEN market_key = 'player_assists' AND outcome_name = 'Under' THEN price END) AS assists_under_price,

        -- Three Pointers Made
        MAX(CASE WHEN market_key = 'player_threes' AND outcome_name = 'Over' THEN point END) AS threes_over_line,
        MAX(CASE WHEN market_key = 'player_threes' AND outcome_name = 'Over' THEN price END) AS threes_over_price,
        MAX(CASE WHEN market_key = 'player_threes' AND outcome_name = 'Under' THEN point END) AS threes_under_line,
        MAX(CASE WHEN market_key = 'player_threes' AND outcome_name = 'Under' THEN price END) AS threes_under_price,

        -- Steals
        MAX(CASE WHEN market_key = 'player_steals' AND outcome_name = 'Over' THEN point END) AS steals_over_line,
        MAX(CASE WHEN market_key = 'player_steals' AND outcome_name = 'Over' THEN price END) AS steals_over_price,
        MAX(CASE WHEN market_key = 'player_steals' AND outcome_name = 'Under' THEN point END) AS steals_under_line,
        MAX(CASE WHEN market_key = 'player_steals' AND outcome_name = 'Under' THEN price END) AS steals_under_price,

        -- Blocks
        MAX(CASE WHEN market_key = 'player_blocks' AND outcome_name = 'Over' THEN point END) AS blocks_over_line,
        MAX(CASE WHEN market_key = 'player_blocks' AND outcome_name = 'Over' THEN price END) AS blocks_over_price,
        MAX(CASE WHEN market_key = 'player_blocks' AND outcome_name = 'Under' THEN point END) AS blocks_under_line,
        MAX(CASE WHEN market_key = 'player_blocks' AND outcome_name = 'Under' THEN price END) AS blocks_under_price,

        -- Turnovers
        MAX(CASE WHEN market_key = 'player_turnovers' AND outcome_name = 'Over' THEN point END) AS turnovers_over_line,
        MAX(CASE WHEN market_key = 'player_turnovers' AND outcome_name = 'Over' THEN price END) AS turnovers_over_price,
        MAX(CASE WHEN market_key = 'player_turnovers' AND outcome_name = 'Under' THEN point END)
            AS turnovers_under_line,
        MAX(CASE WHEN market_key = 'player_turnovers' AND outcome_name = 'Under' THEN price END)
            AS turnovers_under_price,

        -- Blocks + Steals (Combo)
        MAX(CASE WHEN market_key = 'player_blocks_steals' AND outcome_name = 'Over' THEN point END)
            AS blocks_steals_over_line,
        MAX(CASE WHEN market_key = 'player_blocks_steals' AND outcome_name = 'Over' THEN price END)
            AS blocks_steals_over_price,
        MAX(CASE WHEN market_key = 'player_blocks_steals' AND outcome_name = 'Under' THEN point END)
            AS blocks_steals_under_line,
        MAX(CASE WHEN market_key = 'player_blocks_steals' AND outcome_name = 'Under' THEN price END)
            AS blocks_steals_under_price,

        -- Combo Stats
        MAX(CASE WHEN market_key = 'player_points_rebounds_assists' AND outcome_name = 'Over' THEN point END)
            AS pra_over_line,
        MAX(CASE WHEN market_key = 'player_points_rebounds_assists' AND outcome_name = 'Over' THEN price END)
            AS pra_over_price,
        MAX(CASE WHEN market_key = 'player_points_rebounds_assists' AND outcome_name = 'Under' THEN point END)
            AS pra_under_line,
        MAX(CASE WHEN market_key = 'player_points_rebounds_assists' AND outcome_name = 'Under' THEN price END)
            AS pra_under_price,

        MAX(CASE WHEN market_key = 'player_points_rebounds' AND outcome_name = 'Over' THEN point END) AS pr_over_line,
        MAX(CASE WHEN market_key = 'player_points_rebounds' AND outcome_name = 'Over' THEN price END) AS pr_over_price,
        MAX(CASE WHEN market_key = 'player_points_rebounds' AND outcome_name = 'Under' THEN point END) AS pr_under_line,
        MAX(CASE WHEN market_key = 'player_points_rebounds' AND outcome_name = 'Under' THEN price END)
            AS pr_under_price,

        MAX(CASE WHEN market_key = 'player_points_assists' AND outcome_name = 'Over' THEN point END) AS pa_over_line,
        MAX(CASE WHEN market_key = 'player_points_assists' AND outcome_name = 'Over' THEN price END) AS pa_over_price,
        MAX(CASE WHEN market_key = 'player_points_assists' AND outcome_name = 'Under' THEN point END) AS pa_under_line,
        MAX(CASE WHEN market_key = 'player_points_assists' AND outcome_name = 'Under' THEN price END) AS pa_under_price,

        MAX(CASE WHEN market_key = 'player_rebounds_assists' AND outcome_name = 'Over' THEN point END) AS ra_over_line,
        MAX(CASE WHEN market_key = 'player_rebounds_assists' AND outcome_name = 'Over' THEN price END) AS ra_over_price,
        MAX(CASE WHEN market_key = 'player_rebounds_assists' AND outcome_name = 'Under' THEN point END)
            AS ra_under_line,
        MAX(CASE WHEN market_key = 'player_rebounds_assists' AND outcome_name = 'Under' THEN price END)
            AS ra_under_price,

        -- Double Double (Yes/No format)
        MAX(CASE WHEN market_key = 'player_double_double' AND outcome_name = 'Yes' THEN price END)
            AS double_double_yes_price,
        MAX(CASE WHEN market_key = 'player_double_double' AND outcome_name = 'No' THEN price END)
            AS double_double_no_price,

        -- Triple Double (Yes/No format)
        MAX(CASE WHEN market_key = 'player_triple_double' AND outcome_name = 'Yes' THEN price END)
            AS triple_double_yes_price,
        MAX(CASE WHEN market_key = 'player_triple_double' AND outcome_name = 'No' THEN price END)
            AS triple_double_no_price

    FROM base_odds
    GROUP BY 1, 2, 3, 4, 5, 6
),

final AS (
    SELECT
        -- Primary dimensions
        event_id,
        bookmaker_key,
        player_name,

        -- Event details
        commence_time,

        -- Timestamps
        snapshot_timestamp,
        extraction_timestamp,

        -- All pivoted columns (tabular format)
        points_over_line,
        points_over_price,
        points_under_line,
        points_under_price,

        rebounds_over_line,
        rebounds_over_price,
        rebounds_under_line,
        rebounds_under_price,

        assists_over_line,
        assists_over_price,
        assists_under_line,
        assists_under_price,

        threes_over_line,
        threes_over_price,
        threes_under_line,
        threes_under_price,

        steals_over_line,
        steals_over_price,
        steals_under_line,
        steals_under_price,

        blocks_over_line,
        blocks_over_price,
        blocks_under_line,
        blocks_under_price,

        turnovers_over_line,
        turnovers_over_price,
        turnovers_under_line,
        turnovers_under_price,

        blocks_steals_over_line,
        blocks_steals_over_price,
        blocks_steals_under_line,
        blocks_steals_under_price,

        pra_over_line,
        pra_over_price,
        pra_under_line,
        pra_under_price,

        pr_over_line,
        pr_over_price,
        pr_under_line,
        pr_under_price,

        pa_over_line,
        pa_over_price,
        pa_under_line,
        pa_under_price,

        ra_over_line,
        ra_over_price,
        ra_under_line,
        ra_under_price,

        double_double_yes_price,
        double_double_no_price,

        triple_double_yes_price,
        triple_double_no_price

    FROM pivoted_odds
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY snapshot_timestamp DESC) = 1
)

SELECT * FROM final
