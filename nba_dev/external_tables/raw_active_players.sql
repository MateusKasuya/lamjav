-- External table for NBA Active Players
-- This table is managed by dbt via sources.yml configuration
-- Location: gs://smartbetting-landing/nba/active_players/{season}/raw_{catalog}_{table}_{season}.json
-- Format: NEWLINE_DELIMITED_JSON

-- Note: The 'team' field is automatically converted to STRUCT by BigQuery
-- Access team fields using dot notation: team.id, team.name, team.abbreviation

-- ============================================================================
-- AMBIENTE: Este script é apenas para referência/documentação
-- O DBT cria as external tables automaticamente via sources.yml usando o
-- projeto configurado na variável de ambiente DBT_PROJECT:
-- - DEV: sigma-heuristic-469419-h3
-- - PROD: smartbetting-dados
-- ============================================================================

CREATE OR REPLACE EXTERNAL TABLE `sigma-heuristic-469419-h3.nba.raw_active_players`
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://smartbetting-landing/nba/active_players/2025/raw_nba_active_players_2025.json']
);

-- Sample queries to test the external table:
-- SELECT * FROM `lamjav.nba.raw_active_players` LIMIT 10;
-- SELECT id, first_name, last_name, team.name as team_name FROM `lamjav.nba.raw_active_players` LIMIT 10;