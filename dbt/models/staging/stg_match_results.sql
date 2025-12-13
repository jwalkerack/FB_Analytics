WITH match_data AS (
    SELECT 
        JSON_VALUE(match_json.value, '$.match_id') AS MATCH_ID,
        match_json.value                           AS match_value
    FROM {{ source('raw_match', 'raw_files') }} AS f
    CROSS APPLY OPENJSON(f.JSON_BODY) AS match_json
),

team_data AS (
    SELECT
        m.MATCH_ID,
        team.[key]                                           AS TEAM_TYPE,     -- 'home_team' or 'away_team'

        JSON_VALUE(team.value, '$.name')                     AS TEAM_NAME,
        JSON_VALUE(team.value, '$.manager')                  AS TEAM_MANAGER,
        JSON_VALUE(team.value, '$.formation')                AS TEAM_FORMATION,

        TRY_CAST(JSON_VALUE(team.value, '$.score') AS float) AS TEAM_SCORE,

        -- possession like '63%' -> 0.63
        TRY_CAST(
            REPLACE(
                JSON_VALUE(team.value, '$.possession'),
                '%',
                ''
            ) AS float
        ) / 100.0                                            AS TEAM_POSSESSION
    FROM match_data AS m
    CROSS APPLY OPENJSON(m.match_value) AS team
    WHERE team.[key] IN ('home_team', 'away_team')
),

match_summary AS (
    SELECT
        t.MATCH_ID,

        -- Home team details
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_NAME      END) AS HOME_TEAM_NAME,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_MANAGER   END) AS HOME_TEAM_MANAGER,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_FORMATION END) AS HOME_TEAM_FORMATION,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_SCORE     END) AS HOME_TEAM_SCORE,
        MAX(CASE WHEN t.TEAM_TYPE = 'home_team' THEN t.TEAM_POSSESSION END) AS HOME_TEAM_POSSESSION,

        -- Away team details
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_NAME      END) AS AWAY_TEAM_NAME,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_MANAGER   END) AS AWAY_TEAM_MANAGER,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_FORMATION END) AS AWAY_TEAM_FORMATION,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_SCORE     END) AS AWAY_TEAM_SCORE,
        MAX(CASE WHEN t.TEAM_TYPE = 'away_team' THEN t.TEAM_POSSESSION END) AS AWAY_TEAM_POSSESSION,

        -- Was the game postponed? (any team with NULL score)
        CASE
            WHEN MAX(CASE WHEN t.TEAM_SCORE IS NULL THEN 1 ELSE 0 END) = 1
                THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
        END AS WAS_GAME_POSTPONED,

        -- Additional match details
        -- Rely on SQL Server's date parsing of the played_on string
        MAX(
            TRY_CONVERT(date, JSON_VALUE(m.match_value, '$.played_on'))
        ) AS PLAYED_ON,

        MAX(JSON_VALUE(m.match_value, '$.League_Name')) AS LEAGUE_NAME,
        MAX(JSON_VALUE(m.match_value, '$.venue'))       AS VENUE,

        MAX(
            TRY_CAST(
                REPLACE(
                    JSON_VALUE(m.match_value, '$.attendance'),
                    ',',
                    ''
                ) AS int
            )
        ) AS ATTENDANCE

    FROM team_data t
    JOIN match_data m ON t.MATCH_ID = m.MATCH_ID
    GROUP BY t.MATCH_ID
)

SELECT *
FROM match_summary;
