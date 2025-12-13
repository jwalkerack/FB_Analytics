{{ config(
    materialized='table'
) }}


SELECT
    match_id,  -- Unique identifier for each football match
    home_team_name,  -- Name of the home team participating in the match
    home_team_manager,  -- Manager of the home team
    home_team_formation,  -- Formation used by the home team (e.g., 4-4-2)
    home_team_score,  -- Final score of the home team
    home_team_possession,  -- Ball possession of the home team (as a decimal)

    away_team_name,  -- Name of the away team
    away_team_manager,  -- Manager of the away team
    away_team_formation,  -- Formation used by the away team
    away_team_score,  -- Final score of the away team
    away_team_possession,  -- Ball possession of the away team (as a decimal)

    was_game_postponed,  -- Boolean flag indicating if the game was postponed
    played_on,  -- Date the match was played
    league_name,  -- Name of the league
    venue,  -- Venue where the match was played
    attendance  -- Number of attendees at the match
FROM {{ ref('stg_match_results') }}
WHERE was_game_postponed = 1  -- Only include matches that were not postponed