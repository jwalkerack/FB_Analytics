{{ config(
    materialized='table'
) }}

WITH unique_matches AS (
    SELECT DISTINCT 
        match_id as bbc_id

    FROM {{ ref('match_results') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY bbc_id) AS game_id, -- Surrogate key
    bbc_id

FROM unique_matches
