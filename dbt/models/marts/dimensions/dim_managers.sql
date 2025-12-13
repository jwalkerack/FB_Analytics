{{ config(
    materialized='table'
) }}

WITH unique_managers AS (
    SELECT DISTINCT home_team_manager AS manager_name FROM {{ ref('match_results') }}
    UNION
    SELECT DISTINCT away_team_manager AS manager_name FROM {{ ref('match_results') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY manager_name) AS manager_id, 
    REPLACE(manager_name, 'Manager: ', '') AS manager_name
FROM unique_managers