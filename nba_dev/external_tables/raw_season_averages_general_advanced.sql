-- External table for NBA season averages from JSON files
-- Using explicit schema to avoid invalid column names
-- Location: gs://smartbetting-landing/nba/season_averages/{category}/{type}/{season_type}/{season}/season_averages_{category}_{type}_{season_type}_{season}.json
-- Format: NEWLINE_DELIMITED_JSON

CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_season_averages_general_advanced`
OPTIONS (
    format = 'NEWLINE_DELIMITED_JSON',
    uris = [
        'gs://smartbetting-landing/nba/season_averages/general/advanced/regular/2024/season_averages_general_advanced_regular_2024.json',
        'gs://smartbetting-landing/nba/season_averages/general/advanced/playoffs/2024/season_averages_general_advanced_playoffs_2024.json',
        'gs://smartbetting-landing/nba/season_averages/general/advanced/ist/2024/season_averages_general_advanced_ist_2024.json',
        'gs://smartbetting-landing/nba/season_averages/general/advanced/playin/2024/season_averages_general_advanced_playin_2024.json'
    ]
);

-- SELECT * FROM `sigma-heuristic-469419-h3.nba.raw_season_averages_general_advanced` LIMIT 5;
