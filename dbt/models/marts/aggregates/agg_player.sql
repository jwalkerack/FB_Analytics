{{ config(materialized='table') }}

WITH player_aggregates AS (
    SELECT
        player_id,
        TEAM_ID,

        -- Total Metrics
        SUM(YellowCards) AS Total_YellowCards,
        SUM(RedCards) AS Total_RedCards,
        SUM(Goals) AS Total_Goals,
        SUM(Assists) AS Total_Assists,
        SUM(MinutesPlayed) AS Total_MinutesPlayed,
        COUNT(DISTINCT GAME_ID) AS Total_Squads_Made,  -- Renamed from Total_Matches_Played

        -- New Metric: Total Match Involvements (Starter or Sub)
        COUNT(DISTINCT CASE 
            WHEN player_matchRole IN ('Starter', 'Sub') THEN GAME_ID 
            ELSE NULL 
        END) AS Total_Match_Involvements,

        -- Role-Based Subtotals: When Starter
        SUM(CASE WHEN player_matchRole = 'Starter' THEN YellowCards ELSE 0 END) AS YellowCards_When_Started,
        SUM(CASE WHEN player_matchRole = 'Starter' THEN RedCards ELSE 0 END) AS RedCards_When_Started,
        SUM(CASE WHEN player_matchRole = 'Starter' THEN Goals ELSE 0 END) AS Goals_When_Started,
        SUM(CASE WHEN player_matchRole = 'Starter' THEN Assists ELSE 0 END) AS Assists_When_Started,
        SUM(CASE WHEN player_matchRole = 'Starter' THEN MinutesPlayed ELSE 0 END) AS MinutesPlayed_When_Started,
        COUNT(DISTINCT CASE WHEN player_matchRole = 'Starter' THEN GAME_ID ELSE NULL END) AS Matches_Started,

        -- Role-Based Subtotals: When Sub
        SUM(CASE WHEN player_matchRole = 'Sub' THEN YellowCards ELSE 0 END) AS YellowCards_When_Sub,
        SUM(CASE WHEN player_matchRole = 'Sub' THEN RedCards ELSE 0 END) AS RedCards_When_Sub,
        SUM(CASE WHEN player_matchRole = 'Sub' THEN Goals ELSE 0 END) AS Goals_When_Sub,
        SUM(CASE WHEN player_matchRole = 'Sub' THEN Assists ELSE 0 END) AS Assists_When_Sub,
        SUM(CASE WHEN player_matchRole = 'Sub' THEN MinutesPlayed ELSE 0 END) AS MinutesPlayed_When_Sub,
        COUNT(DISTINCT CASE WHEN player_matchRole = 'Sub' THEN GAME_ID ELSE NULL END) AS Matches_As_Sub,

        -- Role-Based Subtotals: When Squad (Did Not Play)
        SUM(CASE WHEN player_matchRole = 'Squad' THEN YellowCards ELSE 0 END) AS YellowCards_When_Squad,
        SUM(CASE WHEN player_matchRole = 'Squad' THEN RedCards ELSE 0 END) AS RedCards_When_Squad,
        SUM(CASE WHEN player_matchRole = 'Squad' THEN Goals ELSE 0 END) AS Goals_When_Squad,
        SUM(CASE WHEN player_matchRole = 'Squad' THEN Assists ELSE 0 END) AS Assists_When_Squad,
        SUM(CASE WHEN player_matchRole = 'Squad' THEN MinutesPlayed ELSE 0 END) AS MinutesPlayed_When_Squad,
        COUNT(DISTINCT CASE WHEN player_matchRole = 'Squad' THEN GAME_ID ELSE NULL END) AS Matches_As_Squad,

        -- Efficiency Metrics
        (SUM(Goals) * 90.0) / NULLIF(SUM(MinutesPlayed), 0) AS Goals_Per_90,
        (SUM(Assists) * 90.0) / NULLIF(SUM(MinutesPlayed), 0) AS Assists_Per_90,
        ((SUM(Goals) + SUM(Assists)) * 90.0) / NULLIF(SUM(MinutesPlayed), 0) AS Goal_Contribution_Per_90,
        ((SUM(YellowCards) + SUM(RedCards)) * 90.0) / NULLIF(SUM(MinutesPlayed), 0) AS Cards_Per_90,

        -- Impact Metrics
        (SUM(CASE WHEN player_matchRole = 'Starter' THEN Goals ELSE 0 END) * 100.0) / NULLIF(SUM(Goals), 0) AS Pct_Goals_When_Started,
        (SUM(CASE WHEN player_matchRole = 'Sub' THEN Goals ELSE 0 END) * 100.0) / NULLIF(SUM(Goals), 0) AS Pct_Goals_When_Sub,
        (SUM(CASE WHEN player_matchRole = 'Squad' THEN Goals ELSE 0 END) * 100.0) / NULLIF(SUM(Goals), 0) AS Pct_Goals_When_Squad

    FROM {{ ref('fact_player_match') }}
    GROUP BY player_id, TEAM_ID
)

SELECT DM.Player_name , PA.* , DT.TEAM_NAME , DT.league_name , DT.Country_name , DT.SHORT_NAME
FROM player_aggregates PA
Left Join {{ ref('dim_players') }} DM on PA.player_id = DM.player_id
Left Join {{ ref('dim_teams') }} DT on PA.TEAM_id = DT.TEAM_ID
