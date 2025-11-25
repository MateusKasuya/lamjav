{{ config(
    description='Staging table for current event odds - basic flattening and cleaning only'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('odds', 'raw_event_odds') }}
),

final AS (
    SELECT
        -- Event information
        id AS event_id,
        cast(commence_time AS timestamp) AS commence_time,

        -- Market information
        market.key AS market_key,
        bookmaker.key AS bookmaker_key,

        -- Outcome information
        outcome.description AS player_name,
        CASE
            WHEN market.key = 'player_double_double' OR market.key = 'player_triple_double' THEN 1
            ELSE cast(outcome.point AS float64)
        END AS line,

        -- Team abbreviation mapping (convert full team names to abbreviations)
        CASE
            WHEN home_team = 'Atlanta Hawks' THEN 'ATL'
            WHEN home_team = 'Boston Celtics' THEN 'BOS'
            WHEN home_team = 'Brooklyn Nets' THEN 'BKN'
            WHEN home_team = 'Charlotte Hornets' THEN 'CHA'
            WHEN home_team = 'Chicago Bulls' THEN 'CHI'
            WHEN home_team = 'Cleveland Cavaliers' THEN 'CLE'
            WHEN home_team = 'Dallas Mavericks' THEN 'DAL'
            WHEN home_team = 'Denver Nuggets' THEN 'DEN'
            WHEN home_team = 'Detroit Pistons' THEN 'DET'
            WHEN home_team = 'Golden State Warriors' THEN 'GSW'
            WHEN home_team = 'Houston Rockets' THEN 'HOU'
            WHEN home_team = 'Indiana Pacers' THEN 'IND'
            WHEN home_team = 'Los Angeles Clippers' THEN 'LAC'
            WHEN home_team = 'Los Angeles Lakers' THEN 'LAL'
            WHEN home_team = 'Memphis Grizzlies' THEN 'MEM'
            WHEN home_team = 'Miami Heat' THEN 'MIA'
            WHEN home_team = 'Milwaukee Bucks' THEN 'MIL'
            WHEN home_team = 'Minnesota Timberwolves' THEN 'MIN'
            WHEN home_team = 'New Orleans Pelicans' THEN 'NOP'
            WHEN home_team = 'New York Knicks' THEN 'NYK'
            WHEN home_team = 'Oklahoma City Thunder' THEN 'OKC'
            WHEN home_team = 'Orlando Magic' THEN 'ORL'
            WHEN home_team = 'Philadelphia 76ers' THEN 'PHI'
            WHEN home_team = 'Phoenix Suns' THEN 'PHX'
            WHEN home_team = 'Portland Trail Blazers' THEN 'POR'
            WHEN home_team = 'Sacramento Kings' THEN 'SAC'
            WHEN home_team = 'San Antonio Spurs' THEN 'SAS'
            WHEN home_team = 'Toronto Raptors' THEN 'TOR'
            WHEN home_team = 'Utah Jazz' THEN 'UTA'
            WHEN home_team = 'Washington Wizards' THEN 'WAS'
            ELSE 'UNK'
        END AS home_team_abbr,

        CASE
            WHEN away_team = 'Atlanta Hawks' THEN 'ATL'
            WHEN away_team = 'Boston Celtics' THEN 'BOS'
            WHEN away_team = 'Brooklyn Nets' THEN 'BKN'
            WHEN away_team = 'Charlotte Hornets' THEN 'CHA'
            WHEN away_team = 'Chicago Bulls' THEN 'CHI'
            WHEN away_team = 'Cleveland Cavaliers' THEN 'CLE'
            WHEN away_team = 'Dallas Mavericks' THEN 'DAL'
            WHEN away_team = 'Denver Nuggets' THEN 'DEN'
            WHEN away_team = 'Detroit Pistons' THEN 'DET'
            WHEN away_team = 'Golden State Warriors' THEN 'GSW'
            WHEN away_team = 'Houston Rockets' THEN 'HOU'
            WHEN away_team = 'Indiana Pacers' THEN 'IND'
            WHEN away_team = 'Los Angeles Clippers' THEN 'LAC'
            WHEN away_team = 'Los Angeles Lakers' THEN 'LAL'
            WHEN away_team = 'Memphis Grizzlies' THEN 'MEM'
            WHEN away_team = 'Miami Heat' THEN 'MIA'
            WHEN away_team = 'Milwaukee Bucks' THEN 'MIL'
            WHEN away_team = 'Minnesota Timberwolves' THEN 'MIN'
            WHEN away_team = 'New Orleans Pelicans' THEN 'NOP'
            WHEN away_team = 'New York Knicks' THEN 'NYK'
            WHEN away_team = 'Oklahoma City Thunder' THEN 'OKC'
            WHEN away_team = 'Orlando Magic' THEN 'ORL'
            WHEN away_team = 'Philadelphia 76ers' THEN 'PHI'
            WHEN away_team = 'Phoenix Suns' THEN 'PHX'
            WHEN away_team = 'Portland Trail Blazers' THEN 'POR'
            WHEN away_team = 'Sacramento Kings' THEN 'SAC'
            WHEN away_team = 'San Antonio Spurs' THEN 'SAS'
            WHEN away_team = 'Toronto Raptors' THEN 'TOR'
            WHEN away_team = 'Utah Jazz' THEN 'UTA'
            WHEN away_team = 'Washington Wizards' THEN 'WAS'
            ELSE 'UNK'
        END AS away_team_abbr,

        -- Metadata
        current_timestamp() AS extraction_timestamp

    FROM source_data,
        unnest(bookmakers) AS bookmaker,
        unnest(bookmaker.markets) AS market,
        unnest(market.outcomes) AS outcome
    WHERE outcome.name = 'Over' OR outcome.name = 'Yes'
)

SELECT
    event_id,
    commence_time,
    market_key,
    bookmaker_key,
    line,
    extraction_timestamp,
    trim(player_name) AS player_name,
    trim(player_name || ' (' || home_team_abbr || ')') AS player_name_home_team,
    trim(player_name || ' (' || away_team_abbr || ')') AS player_name_away_team
FROM final
QUALIFY row_number() OVER (PARTITION BY player_name, market_key ORDER BY commence_time DESC) = 1
