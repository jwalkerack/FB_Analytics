{{ config(materialized='view') }}


SELECT DT.*, FML.attendance , FML.played_on 
FROM {{ ref('fact_match_light') }} as FML 
left join {{ ref('dim_teams') }} DT on FML.Home_Team_ID = DT.TEAM_ID