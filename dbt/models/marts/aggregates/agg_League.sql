{{ config(materialized='table') }}

WITH team_data AS (
    SELECT 
        ftm.TEAM_ID,
        dt.TEAM_NAME,
        dt.LEAGUE_NAME as Formal_League_Name,
        dt.COUNTRY_NAME,
        dt.SHORT_NAME as short_League_Name,
        ftm.GameRole,
        ftm.scored,
        ftm.conceeded,
        ftm.Points
    FROM {{ ref('fact_team_match') }} ftm
    LEFT JOIN {{ ref('dim_teams') }} dt ON ftm.TEAM_ID = dt.TEAM_ID
)

SELECT
    TEAM_ID,
    TEAM_NAME,
    Formal_League_Name,
    COUNTRY_NAME,
    short_League_Name,
    
    -- Overall Totals
    COUNT(*) AS GamesPlayed,
    SUM(Points) AS TotalPoints,  
    SUM(CASE WHEN Points = 3 THEN 1 ELSE 0 END) AS Wins,
    SUM(CASE WHEN Points = 0 THEN 1 ELSE 0 END) AS Losses,
    SUM(CASE WHEN Points = 1 THEN 1 ELSE 0 END) AS Draws,
    SUM(scored) AS TotalScored,
    SUM(conceeded) AS TotalConceded,
    -- Home Game Totals
    SUM(CASE WHEN GameRole = 1 THEN 1 ELSE 0 END) AS HomeGamesPlayed,
    SUM(CASE WHEN GameRole = 1 AND Points = 3 THEN 1 ELSE 0 END) AS HomeWins,
    SUM(CASE WHEN GameRole = 1 AND Points = 0 THEN 1 ELSE 0 END) AS HomeLosses,
    SUM(CASE WHEN GameRole = 1 AND Points = 1 THEN 1 ELSE 0 END) AS HomeDraws,
    SUM(CASE WHEN GameRole = 1 THEN scored ELSE 0 END) AS HomeScored,
    SUM(CASE WHEN GameRole = 1 THEN conceeded ELSE 0 END) AS HomeConceded,
    SUM(CASE WHEN GameRole = 1 THEN Points ELSE 0 END) AS HomePoints,
    
    -- Away Game Totals
    SUM(CASE WHEN GameRole = 2 THEN 1 ELSE 0 END) AS AwayGamesPlayed,
    SUM(CASE WHEN GameRole = 2 AND Points = 3 THEN 1 ELSE 0 END) AS AwayWins,
    SUM(CASE WHEN GameRole = 2 AND Points = 0 THEN 1 ELSE 0 END) AS AwayLosses,
    SUM(CASE WHEN GameRole = 2 AND Points = 1 THEN 1 ELSE 0 END) AS AwayDraws,
    SUM(CASE WHEN GameRole = 2 THEN scored ELSE 0 END) AS AwayScored,
    SUM(CASE WHEN GameRole = 2 THEN conceeded ELSE 0 END) AS AwayConceded,
    SUM(CASE WHEN GameRole = 2 THEN Points ELSE 0 END) AS AwayPoints

FROM team_data
GROUP BY TEAM_ID, TEAM_NAME, Formal_League_Name, COUNTRY_NAME, short_League_Name

