{{ config(
    materialized='table'
) }}

WITH base_teams AS (
    SELECT 
        TRIM(UPPER(home_team_name)) AS team_name, -- Normalize team names
        TRIM(UPPER(REPLACE(home_team_manager, 'Manager: ', ''))) AS manager_name, -- Clean manager names
        PLAYED_ON
    FROM {{ ref('match_results') }}
    WHERE WAS_GAME_POSTPONED = 0 OR WAS_GAME_POSTPONED IS NULL
    UNION ALL
    SELECT 
        TRIM(UPPER(away_team_name)) AS team_name, -- Normalize team names
        TRIM(UPPER(REPLACE(away_team_manager, 'Manager: ', ''))) AS manager_name, -- Clean manager names
        PLAYED_ON
    FROM {{ ref('match_results') }}
    WHERE WAS_GAME_POSTPONED = 0 OR WAS_GAME_POSTPONED IS NULL
),

team_with_change_group AS (
    SELECT 
        team_name,
        manager_name,
        PLAYED_ON,
        CASE 
            WHEN manager_name != LAG(manager_name) OVER (PARTITION BY team_name ORDER BY PLAYED_ON) THEN 1
            ELSE 0
        END AS manager_change_flag
    FROM base_teams
),

change_groups AS (
    SELECT 
        team_name,
        manager_name,
        PLAYED_ON,
        SUM(manager_change_flag) OVER (PARTITION BY team_name ORDER BY PLAYED_ON) AS change_group
    FROM team_with_change_group
),

manager_tenure AS (
    SELECT 
        team_name,
        manager_name,
        MIN(PLAYED_ON) AS effective_date,
        COALESCE(
            LEAD(MIN(PLAYED_ON)) OVER (PARTITION BY team_name ORDER BY MIN(PLAYED_ON)), 
            '9999-12-31'
        ) AS end_date
    FROM change_groups
    GROUP BY team_name, manager_name, change_group
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY t.team_name, mt.effective_date) AS team_manager_id, -- Surrogate Key
    t.team_id,
    m.manager_id,
    mt.effective_date,
    mt.end_date,
    CASE WHEN mt.end_date = '9999-12-31' THEN 1 ELSE 0 END AS is_current
FROM manager_tenure mt
JOIN {{ ref('dim_teams') }} t ON TRIM(UPPER(mt.team_name)) = TRIM(UPPER(t.team_name))
JOIN {{ ref('dim_managers') }} m ON TRIM(UPPER(mt.manager_name)) = TRIM(UPPER(m.manager_name))
