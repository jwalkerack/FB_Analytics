{{ config(materialized='table') }}

WITH normalized_manager_data AS (
    -- Home Manager Data
    SELECT
        MD.GAME_ID,
        DMH.manager_id AS manager_id,
        md.HOME_TEAM_ID as Team_ID,
        'home' AS game_role,
        MD.played_on,
        CASE 
            WHEN MD.gameOut = 'home win' THEN 'win'
            WHEN MD.gameOut = 'away win' THEN 'loss'
            WHEN MD.gameOut = 'draw' THEN 'draw'
            ELSE 'unknown'
        END AS result
    FROM {{ ref('fact_match_light') }} MD
    LEFT JOIN {{ ref('dim_managers') }} DMH ON MD.home_manager_id = DMH.manager_id

    UNION ALL

    -- Away Manager Data
    SELECT
        MD.GAME_ID,
        DMA.manager_id AS manager_id,
        md.AWAY_TEAM_ID as team_id ,
        'away' AS game_role,
        MD.played_on,
        CASE 
            WHEN MD.gameOut = 'away win' THEN 'win'
            WHEN MD.gameOut = 'home win' THEN 'loss'
            WHEN MD.gameOut = 'draw' THEN 'draw'
            ELSE 'unknown'
        END AS result
    FROM {{ ref('fact_match_light') }} MD
    LEFT JOIN {{ ref('dim_managers') }} DMA ON MD.away_manager_id = DMA.manager_id
),

manager_agg AS (
    SELECT
        manager_id,team_id,

        -- Total Games Played
        COUNT(*) AS total_games_played,
        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) AS total_losses,
        SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END) AS total_draws,

        -- Home Games Played
        SUM(CASE WHEN game_role = 'home' THEN 1 ELSE 0 END) AS home_games_played,
        SUM(CASE WHEN game_role = 'home' AND result = 'win' THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN game_role = 'home' AND result = 'loss' THEN 1 ELSE 0 END) AS home_losses,
        SUM(CASE WHEN game_role = 'home' AND result = 'draw' THEN 1 ELSE 0 END) AS home_draws,

        -- Away Games Played
        SUM(CASE WHEN game_role = 'away' THEN 1 ELSE 0 END) AS away_games_played,
        SUM(CASE WHEN game_role = 'away' AND result = 'win' THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN game_role = 'away' AND result = 'loss' THEN 1 ELSE 0 END) AS away_losses,
        SUM(CASE WHEN game_role = 'away' AND result = 'draw' THEN 1 ELSE 0 END) AS away_draws
    FROM normalized_manager_data
    GROUP BY manager_id,team_id
),

manager_percentages AS (
    SELECT
        ma.manager_id,ma.team_id,


        -- Total Games Percentages
        total_games_played,
        ROUND((total_wins * 100.0) / total_games_played, 2) AS games_won_pct,
        ROUND((total_losses * 100.0) / total_games_played, 2) AS games_lost_pct,
        ROUND((total_draws * 100.0) / total_games_played, 2) AS games_drawn_pct,

        -- Home Games Percentages
        home_games_played,
        ROUND((home_wins * 100.0) / NULLIF(home_games_played, 0), 2) AS home_games_won_pct,
        ROUND((home_losses * 100.0) / NULLIF(home_games_played, 0), 2) AS home_games_lost_pct,
        ROUND((home_draws * 100.0) / NULLIF(home_games_played, 0), 2) AS home_games_drawn_pct,

        -- Away Games Percentages
        away_games_played,
        ROUND((away_wins * 100.0) / NULLIF(away_games_played, 0), 2) AS away_games_won_pct,
        ROUND((away_losses * 100.0) / NULLIF(away_games_played, 0), 2) AS away_games_lost_pct,
        ROUND((away_draws * 100.0) / NULLIF(away_games_played, 0), 2) AS away_games_drawn_pct
    FROM manager_agg ma
)

SELECT 
    dm.manager_id,
    dt.*,
    dm.manager_name,
    mp.total_games_played,
    mp.games_won_pct,
    mp.games_lost_pct,
    mp.games_drawn_pct,
    
    mp.home_games_played,
    mp.home_games_won_pct,
    mp.home_games_lost_pct,
    mp.home_games_drawn_pct,
    
    mp.away_games_played,
    mp.away_games_won_pct,
    mp.away_games_lost_pct,
    mp.away_games_drawn_pct

FROM manager_percentages mp
LEFT JOIN {{ ref('dim_managers') }} dm ON mp.manager_id = dm.manager_id
LEFT JOIN {{ ref('dim_teams') }} DT  ON mp.team_id = DT.team_id

