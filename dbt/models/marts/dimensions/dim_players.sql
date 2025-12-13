{{ config(
    materialized='table'
) }}

WITH unique_players AS (
    SELECT DISTINCT 
        -- Normalise to one collation (use your DB’s default from the error)
        player_name COLLATE SQL_Latin1_General_CP1_CI_AS AS player_name,
        team_name   COLLATE SQL_Latin1_General_CP1_CI_AS AS team_name,
        team_number
    FROM {{ ref('players') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY player_name, team_name) AS player_id,   -- Surrogate key
    CONCAT(
        player_name COLLATE SQL_Latin1_General_CP1_CI_AS,
        ' - ',
        team_name   COLLATE SQL_Latin1_General_CP1_CI_AS
    ) AS player_key,                                                    -- Composite key
    player_name,
    team_name,
    team_number
FROM unique_players;
