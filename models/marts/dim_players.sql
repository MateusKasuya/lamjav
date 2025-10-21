{{
  config(
    description='NBA players for analysis',
    labels={'domain': 'bi', 'category': 'analytics'}
  )
}}

WITH last_games_text AS (
    SELECT
        player_id,
        MAX(game_date) AS last_game_date
    FROM
        {{ ref('stg_game_player_stats') }}
    WHERE
        minutes > 0
        AND game_date < '2025-04-07'
    GROUP BY
        player_id
),

game_player_stats AS (
    SELECT
        player_id,
        CONCAT(
            'Ultimo Jogo: ',
            CAST(DATE_DIFF(DATE '2025-04-07', last_game_date, DAY) AS STRING),
            ' ',
            CASE
                WHEN DATE_DIFF(DATE '2025-04-07', last_game_date, DAY) = 1 THEN 'dia atras'
                ELSE 'dias atras'
            END
        ) AS last_game_text
    FROM
        last_games_text
),

injury_report AS (
    SELECT
        player_name,
        current_status
    FROM
        {{ ref('stg_injury_report') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY injury_report_date DESC) = 1
),

last_games AS (
    SELECT
        team_id,
        STRING_AGG(win_loss, ' ' ORDER BY game_id) AS last_games
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_id DESC) AS row_num
        FROM (
            SELECT DISTINCT
                g.team_id,
                g.game_id,
                g.win_loss
            FROM
                {{ ref('stg_game_player_stats') }} AS g
            WHERE g.game_date < '2025-04-07'
        )
    )
    WHERE row_num <= 5
    GROUP BY team_id
),

-- Next games information - simplified
next_games AS (
    SELECT
        home_team_id,
        home_team_name,
        home_team_abbreviation,
        visitor_team_id,
        visitor_team_name,
        visitor_team_abbreviation
    FROM {{ ref('stg_games') }}
    WHERE is_next_game = true
),

team_rating AS (
    SELECT
        team_id,
        team_offensive_rating,
        team_defensive_rating,
        loaded_at,
        team_offensive_rating - team_defensive_rating AS net_rating,
        ROW_NUMBER() OVER (ORDER BY (team_offensive_rating - team_defensive_rating) DESC) AS team_rating_rank,
        ROW_NUMBER() OVER (ORDER BY team_offensive_rating DESC) AS team_offensive_rating_rank,
        ROW_NUMBER() OVER (ORDER BY team_defensive_rating DESC) AS team_defensive_rating_rank
    FROM (
        SELECT
            ap.team_id,
            AVG(sga.offensive_rating) AS team_offensive_rating,
            AVG(sga.defensive_rating) AS team_defensive_rating,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM {{ ref('stg_season_averages_general_advanced') }} AS sga
        INNER JOIN {{ ref('stg_active_players') }} AS ap ON sga.player_id = ap.id
        GROUP BY ap.team_id
    )
)

SELECT
    p.id,
    p.name,
    p.position,
    p.team_id,
    p.team_name,
    p.team_abbreviation,
    s.age,
    s.games_played,
    s.minutes,
    g.last_game_text,
    l.last_games,
    ir.current_status,
    ts.conference_rank,
    tr.team_rating_rank,
    tr.team_offensive_rating_rank,
    tr.team_defensive_rating_rank,
    l2.last_games AS next_opponent_last_games,
    ts2.conference_rank AS next_opponent_conference_rank,
    tr2.team_rating_rank AS next_opponent_team_rating_rank,
    tr2.team_offensive_rating_rank AS next_opponent_team_offensive_rating_rank,
    tr2.team_defensive_rating_rank AS next_opponent_team_defensive_rating_rank,
    CASE
        WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_id
        WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_id
    END AS next_opponent_id,
    CASE
        WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_name
        WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_name
    END AS next_opponent_name,
    CASE
        WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_abbreviation
        WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_abbreviation
    END AS next_opponent_abbreviation,
    CURRENT_TIMESTAMP() AS loaded_at
FROM
    {{ ref('stg_active_players') }} AS p
LEFT JOIN
    {{ ref('stg_season_averages_general_base') }} AS s
    ON p.id = s.player_id
LEFT JOIN game_player_stats AS g ON p.id = g.player_id
LEFT JOIN {{ source('bi_dev', 'de_para_nba_injury_players') }} AS de_para
    ON p.id = de_para.nba_player_id
LEFT JOIN injury_report AS ir
    ON de_para.injury_player_name = ir.player_name
LEFT JOIN last_games AS l ON p.team_id = l.team_id
LEFT JOIN {{ ref('stg_team_standings') }} AS ts ON p.team_id = ts.team_id
-- Next opponent logic using CASE statements
LEFT JOIN next_games AS ng_home ON p.team_id = ng_home.home_team_id
LEFT JOIN next_games AS ng_visitor ON p.team_id = ng_visitor.visitor_team_id
-- Next opponent data
LEFT JOIN last_games AS l2
    ON l2.team_id = CASE
        WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_id
        WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_id
    END
LEFT JOIN {{ ref('stg_team_standings') }} AS ts2
    ON ts2.team_id = CASE
        WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_id
        WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_id
    END
LEFT JOIN team_rating AS tr ON p.team_id = tr.team_id
LEFT JOIN team_rating AS tr2 ON tr2.team_id = CASE
    WHEN ng_home.home_team_id IS NOT null THEN ng_home.visitor_team_id
    WHEN ng_visitor.visitor_team_id IS NOT null THEN ng_visitor.home_team_id
END
