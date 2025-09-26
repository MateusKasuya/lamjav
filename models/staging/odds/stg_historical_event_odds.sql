{{ config(
    materialized='view',
    description='Staging table for historical event odds - basic flattening and cleaning only'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('odds', 'raw_historical_event_odds') }}
),

final AS (
    SELECT
        -- Event information
        data.id AS event_id,
        cast(data.commence_time AS timestamp) AS commence_time,
        data.home_team,
        data.away_team,

        -- Timestamp information
        cast(timestamp AS timestamp) AS snapshot_timestamp,

        -- Bookmaker information
        bookmaker.key AS bookmaker_key,

        -- Market information
        market.key AS market_key,

        -- Outcome information
        outcome.name AS outcome_name,
        outcome.description AS player_name,
        cast(outcome.point AS float64) AS point,
        cast(outcome.price AS float64) AS price,

        -- Team abbreviation mapping (convert full team names to abbreviations)
        CASE
            WHEN data.home_team = 'Atlanta Hawks' THEN 'ATL'
            WHEN data.home_team = 'Boston Celtics' THEN 'BOS'
            WHEN data.home_team = 'Brooklyn Nets' THEN 'BKN'
            WHEN data.home_team = 'Charlotte Hornets' THEN 'CHA'
            WHEN data.home_team = 'Chicago Bulls' THEN 'CHI'
            WHEN data.home_team = 'Cleveland Cavaliers' THEN 'CLE'
            WHEN data.home_team = 'Dallas Mavericks' THEN 'DAL'
            WHEN data.home_team = 'Denver Nuggets' THEN 'DEN'
            WHEN data.home_team = 'Detroit Pistons' THEN 'DET'
            WHEN data.home_team = 'Golden State Warriors' THEN 'GSW'
            WHEN data.home_team = 'Houston Rockets' THEN 'HOU'
            WHEN data.home_team = 'Indiana Pacers' THEN 'IND'
            WHEN data.home_team = 'Los Angeles Clippers' THEN 'LAC'
            WHEN data.home_team = 'Los Angeles Lakers' THEN 'LAL'
            WHEN data.home_team = 'Memphis Grizzlies' THEN 'MEM'
            WHEN data.home_team = 'Miami Heat' THEN 'MIA'
            WHEN data.home_team = 'Milwaukee Bucks' THEN 'MIL'
            WHEN data.home_team = 'Minnesota Timberwolves' THEN 'MIN'
            WHEN data.home_team = 'New Orleans Pelicans' THEN 'NOP'
            WHEN data.home_team = 'New York Knicks' THEN 'NYK'
            WHEN data.home_team = 'Oklahoma City Thunder' THEN 'OKC'
            WHEN data.home_team = 'Orlando Magic' THEN 'ORL'
            WHEN data.home_team = 'Philadelphia 76ers' THEN 'PHI'
            WHEN data.home_team = 'Phoenix Suns' THEN 'PHX'
            WHEN data.home_team = 'Portland Trail Blazers' THEN 'POR'
            WHEN data.home_team = 'Sacramento Kings' THEN 'SAC'
            WHEN data.home_team = 'San Antonio Spurs' THEN 'SAS'
            WHEN data.home_team = 'Toronto Raptors' THEN 'TOR'
            WHEN data.home_team = 'Utah Jazz' THEN 'UTA'
            WHEN data.home_team = 'Washington Wizards' THEN 'WAS'
            ELSE 'UNK'
        END AS home_team_abbr,

        CASE
            WHEN data.away_team = 'Atlanta Hawks' THEN 'ATL'
            WHEN data.away_team = 'Boston Celtics' THEN 'BOS'
            WHEN data.away_team = 'Brooklyn Nets' THEN 'BKN'
            WHEN data.away_team = 'Charlotte Hornets' THEN 'CHA'
            WHEN data.away_team = 'Chicago Bulls' THEN 'CHI'
            WHEN data.away_team = 'Cleveland Cavaliers' THEN 'CLE'
            WHEN data.away_team = 'Dallas Mavericks' THEN 'DAL'
            WHEN data.away_team = 'Denver Nuggets' THEN 'DEN'
            WHEN data.away_team = 'Detroit Pistons' THEN 'DET'
            WHEN data.away_team = 'Golden State Warriors' THEN 'GSW'
            WHEN data.away_team = 'Houston Rockets' THEN 'HOU'
            WHEN data.away_team = 'Indiana Pacers' THEN 'IND'
            WHEN data.away_team = 'Los Angeles Clippers' THEN 'LAC'
            WHEN data.away_team = 'Los Angeles Lakers' THEN 'LAL'
            WHEN data.away_team = 'Memphis Grizzlies' THEN 'MEM'
            WHEN data.away_team = 'Miami Heat' THEN 'MIA'
            WHEN data.away_team = 'Milwaukee Bucks' THEN 'MIL'
            WHEN data.away_team = 'Minnesota Timberwolves' THEN 'MIN'
            WHEN data.away_team = 'New Orleans Pelicans' THEN 'NOP'
            WHEN data.away_team = 'New York Knicks' THEN 'NYK'
            WHEN data.away_team = 'Oklahoma City Thunder' THEN 'OKC'
            WHEN data.away_team = 'Orlando Magic' THEN 'ORL'
            WHEN data.away_team = 'Philadelphia 76ers' THEN 'PHI'
            WHEN data.away_team = 'Phoenix Suns' THEN 'PHX'
            WHEN data.away_team = 'Portland Trail Blazers' THEN 'POR'
            WHEN data.away_team = 'Sacramento Kings' THEN 'SAC'
            WHEN data.away_team = 'San Antonio Spurs' THEN 'SAS'
            WHEN data.away_team = 'Toronto Raptors' THEN 'TOR'
            WHEN data.away_team = 'Utah Jazz' THEN 'UTA'
            WHEN data.away_team = 'Washington Wizards' THEN 'WAS'
            ELSE 'UNK'
        END AS away_team_abbr,

        -- Metadata
        current_timestamp() AS extraction_timestamp

    FROM source_data,
        unnest(data.bookmakers) AS bookmaker,
        unnest(bookmaker.markets) AS market,
        unnest(market.outcomes) AS outcome
)

SELECT * FROM final
