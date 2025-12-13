{{ config(materialized='table') }}

WITH team_match AS (
    SELECT  MD.GAME_ID , DV.venue_id,
    DTH.TEAM_ID AS HOME_TEAM_ID ,DTA.TEAM_ID AS AWAY_TEAM_ID ,
    DMH.manager_id AS home_manager_id,DMA.manager_id AS away_manager_id,
    R.attendance, R.played_on,
    CASE 
        WHEN R.home_team_score > R.away_team_score THEN 'home win'  -- Win
        WHEN R.home_team_score < R.away_team_score THEN 'away win'  -- Loss
        WHEN R.home_team_score = R.away_team_score THEN 'draw' -- Draw
        ELSE 'unkown'
    END AS gameOut

    FROM {{ ref('match_results') }} R
    Left Join {{ ref('dim_match') }} MD on R.MATCH_ID  = MD.BBC_ID 
    Left Join {{ ref('dim_teams') }} DTA on R.away_TEAM_NAME  = DTA.TEAM_NAME
    Left Join {{ ref('dim_teams') }} DTH on R.HOME_TEAM_NAME  = DTH.TEAM_NAME
    Left Join {{ ref('dim_managers') }} DMH on REPLACE(R.HOME_TEAM_MANAGER, 'Manager: ', '')  = DMH.MANAGER_NAME
    Left Join {{ ref('dim_managers') }} DMA on REPLACE(R.AWAY_TEAM_MANAGER, 'Manager: ', '')  = DMA.MANAGER_NAME
    Left Join {{ ref('dim_venues') }} DV on R.Venue = DV.Venue_name

)

SELECT ROW_NUMBER() OVER (ORDER BY played_on)  AS row_id,
* FROM team_match

