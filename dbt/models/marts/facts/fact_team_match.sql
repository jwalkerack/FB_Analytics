{{ config(materialized='table') }}

WITH team_match AS (
    SELECT
        md.game_id,
        dt.team_id,
        mr.home_team_score        AS scored,
        mr.away_team_score        AS conceeded,
        mr.home_team_possession   AS possession,
        REPLACE(mr.home_team_formation, 'Formation: ', '') AS formation,
        1 AS GameRole,
        CASE
            WHEN mr.home_team_score > mr.away_team_score THEN 3  -- Win
            WHEN mr.home_team_score < mr.away_team_score THEN 0  -- Loss
            WHEN mr.home_team_score = mr.away_team_score THEN 1  -- Draw
            ELSE 0
        END AS Points,
        mr.played_on
    FROM {{ ref('match_results') }} AS mr
    LEFT JOIN {{ ref('dim_match') }} AS md
        ON mr.match_id = md.bbc_id
    LEFT JOIN {{ ref('dim_teams') }} AS dt
        ON mr.home_team_name = dt.team_name

    UNION ALL

    SELECT
        md.game_id,
        dt.team_id,
        mr.away_team_score        AS scored,
        mr.home_team_score        AS conceeded,
        mr.away_team_possession   AS possession,
        REPLACE(mr.away_team_formation, 'Formation: ', '') AS formation,
        2 AS GameRole,
        CASE
            WHEN mr.home_team_score > mr.away_team_score THEN 0  -- Loss
            WHEN mr.home_team_score < mr.away_team_score THEN 3  -- Win
            WHEN mr.home_team_score = mr.away_team_score THEN 1  -- Draw
            ELSE 0
        END AS Points,
        mr.played_on
    FROM {{ ref('match_results') }} AS mr
    LEFT JOIN {{ ref('dim_match') }} AS md
        ON mr.match_id = md.bbc_id
    LEFT JOIN {{ ref('dim_teams') }} AS dt
        ON mr.away_team_name = dt.team_name
)

SELECT
    ROW_NUMBER() OVER (ORDER BY played_on) AS row_id,
    *,
    -- Total games (home + away) per team
    ROW_NUMBER() OVER (
        PARTITION BY team_id
        ORDER BY played_on ASC
    ) AS game_number,

    -- Running total of home games
    CASE
        WHEN GameRole = 1 THEN
            SUM(1) OVER (
                PARTITION BY team_id, GameRole
                ORDER BY played_on ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ELSE NULL
    END AS home_game_number,

    -- Running total of away games
    CASE
        WHEN GameRole = 2 THEN
            SUM(1) OVER (
                PARTITION BY team_id, GameRole
                ORDER BY played_on ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        ELSE NULL
    END AS away_game_number

FROM team_match;

