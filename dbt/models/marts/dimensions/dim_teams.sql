{{ config(
    materialized='table'
) }}

WITH unique_teams AS (
    -- Extract distinct team and league names
    SELECT DISTINCT home_team_name AS team_name, league_name
    FROM {{ ref('match_results') }}

    UNION

    SELECT DISTINCT away_team_name AS team_name, league_name
    FROM {{ ref('match_results') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY team_name) AS team_id, -- Generate surrogate key
    team_name,
    league_name,

    -- First word of league name (before first space)
    CASE
        WHEN CHARINDEX(' ', league_name) > 0 THEN
            LEFT(league_name, CHARINDEX(' ', league_name) - 1)
        ELSE
            league_name
    END AS country_name,

    -- Everything after the first space (trimmed); NULL if no space
    CASE
        WHEN CHARINDEX(' ', league_name) > 0 THEN
            LTRIM(RTRIM(
                SUBSTRING(
                    league_name,
                    CHARINDEX(' ', league_name) + 1,
                    LEN(league_name)
                )
            ))
        ELSE
            NULL
    END AS short_name

FROM unique_teams;