{{ config(
    materialized='table'
) }}

WITH league_name AS (
    -- Extract distinct league names
    SELECT DISTINCT league_name
    FROM {{ ref('match_results') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY league_name DESC) AS league_id,  -- Surrogate key
    league_name,

    -- First word of league_name (before first space)
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

FROM league_name;
