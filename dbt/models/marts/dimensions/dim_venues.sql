{{ config(
    materialized='table'
) }}

WITH unique_venues AS (
    SELECT DISTINCT VENUE AS venue_name
    FROM {{ ref('match_results') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY venue_name) AS venue_id, -- Generate surrogate key
    venue_name,
    NULL AS capacity,   -- Placeholder for venue capacity
    NULL AS metoffice   -- Placeholder for MetOffice identifier
FROM unique_venues
