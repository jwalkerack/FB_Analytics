WITH match_data AS (
    SELECT
        JSON_VALUE(match_json.value, '$.match_id') AS match_id,
        match_json.value                           AS match_value
    FROM {{ source('raw_match', 'raw_files') }} AS f
    CROSS APPLY OPENJSON(f.JSON_BODY) AS match_json
),

home_raw AS (
    SELECT
        -- `key` is reserved in SQL Server, so quote it
        player.[key]                                            AS PLAYER_NAME,
        m.match_id                                              AS MATCH_ID,

        JSON_VALUE(m.match_value, '$.home_team.name')           AS TEAM_NAME,

        JSON_VALUE(player.value, '$.ShirtNumber')               AS TEAM_NUMBER,
        CAST(
            REPLACE(
                JSON_VALUE(player.value, '$.ShirtNumber'),
                ',',
                ''
            ) AS INT
        )                                                       AS TEAM_NUMBER1,

        TRY_CAST(JSON_VALUE(player.value, '$.WasStarter')      AS bit)   AS STARTED_GAME,
        TRY_CAST(JSON_VALUE(player.value, '$.WasSubstituted')  AS bit)   AS WAS_SUBSTITUTED,
        TRY_CAST(JSON_VALUE(player.value, '$.WasIntroduced')   AS bit)   AS WAS_INTRODUCED,
        TRY_CAST(JSON_VALUE(player.value, '$.is_captain')      AS bit)   AS is_captain,

        JSON_VALUE(player.value, '$.ReplacedBy')                AS REPLACED_BY,
        JSON_VALUE(player.value, '$.SubstitutionTime')          AS SubstitutionTime,

        JSON_VALUE(player.value, '$.YellowCards')               AS YELLOW_CARDS,
        ycm.YellowCardMinutesStr                                AS YELLOW_CARD_MINUTES,

        TRY_CAST(JSON_VALUE(player.value, '$.RedCards') AS float)       AS RED_CARDS,
        rcm.RedCardMinutesStr                                         AS RED_CARD_MINUTES,

        COALESCE(g.GoalsCount, 0)                             AS GOALS_COUNT,
        g.GoalsArrayStr                                       AS GOALS_ARRAY,

        COALESCE(a.AssistsCount, 0)                           AS ASSISTS_COUNT,
        a.AssistsArrayStr                                     AS ASSISTS_ARRAY,

        TRY_CAST(JSON_VALUE(player.value, '$.MinutesPlayed') AS float)  AS MINUTES_PLAYED,

        'home'                                                AS PLAYING_AS
    FROM match_data AS m
    CROSS APPLY OPENJSON(m.match_value, '$.home_team.players') AS player

    OUTER APPLY (
        SELECT
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS YellowCardMinutesStr
        FROM OPENJSON(player.value, '$.YellowCardMinutes') AS j
    ) AS ycm

    OUTER APPLY (
        SELECT
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS RedCardMinutesStr
        FROM OPENJSON(player.value, '$.RedCardMinutes') AS j
    ) AS rcm

    OUTER APPLY (
        SELECT
            COUNT(*)                                        AS GoalsCount,
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS GoalsArrayStr
        FROM OPENJSON(player.value, '$.Goals') AS j
    ) AS g

    OUTER APPLY (
        SELECT
            COUNT(*)                                         AS AssistsCount,
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS AssistsArrayStr
        FROM OPENJSON(player.value, '$.Assists') AS j
    ) AS a
),

home_players AS (
    SELECT
        *,
        CASE
            WHEN STARTED_GAME = 1 AND WAS_SUBSTITUTED = 0 THEN 'Played Full Game'
            WHEN STARTED_GAME = 1 AND WAS_SUBSTITUTED = 1 THEN 'Played Subbed Off'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 1 AND WAS_SUBSTITUTED = 1 THEN 'Played Subbed On and Subbed Off'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 1 AND WAS_SUBSTITUTED = 0 THEN 'Played Subbed On'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 0 THEN 'Did Not Play'
            ELSE 'Unknown Status'
        END AS PLAYER_STATUS
    FROM home_raw
),

away_raw AS (
    SELECT
        player.[key]                                            AS PLAYER_NAME,
        m.match_id                                              AS MATCH_ID,

        JSON_VALUE(m.match_value, '$.away_team.name')           AS TEAM_NAME,

        JSON_VALUE(player.value, '$.ShirtNumber')               AS TEAM_NUMBER,
        CAST(
            REPLACE(
                JSON_VALUE(player.value, '$.ShirtNumber'),
                ',',
                ''
            ) AS INT
        )                                                       AS TEAM_NUMBER1,

        TRY_CAST(JSON_VALUE(player.value, '$.WasStarter')      AS bit)   AS STARTED_GAME,
        TRY_CAST(JSON_VALUE(player.value, '$.WasSubstituted')  AS bit)   AS WAS_SUBSTITUTED,
        TRY_CAST(JSON_VALUE(player.value, '$.WasIntroduced')   AS bit)   AS WAS_INTRODUCED,
        TRY_CAST(JSON_VALUE(player.value, '$.is_captain')      AS bit)   AS is_captain,

        JSON_VALUE(player.value, '$.ReplacedBy')                AS REPLACED_BY,
        JSON_VALUE(player.value, '$.SubstitutionTime')          AS SubstitutionTime,

        JSON_VALUE(player.value, '$.YellowCards')               AS YELLOW_CARDS,
        ycm.YellowCardMinutesStr                                AS YELLOW_CARD_MINUTES,

        TRY_CAST(JSON_VALUE(player.value, '$.RedCards') AS float)       AS RED_CARDS,
        rcm.RedCardMinutesStr                                         AS RED_CARD_MINUTES,

        COALESCE(g.GoalsCount, 0)                             AS GOALS_COUNT,
        g.GoalsArrayStr                                       AS GOALS_ARRAY,

        COALESCE(a.AssistsCount, 0)                           AS ASSISTS_COUNT,
        a.AssistsArrayStr                                     AS ASSISTS_ARRAY,

        TRY_CAST(JSON_VALUE(player.value, '$.MinutesPlayed') AS float)  AS MINUTES_PLAYED,

        'away'                                                AS PLAYING_AS
    FROM match_data AS m
    CROSS APPLY OPENJSON(m.match_value, '$.away_team.players') AS player

    OUTER APPLY (
        SELECT
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS YellowCardMinutesStr
        FROM OPENJSON(player.value, '$.YellowCardMinutes') AS j
    ) AS ycm

    OUTER APPLY (
        SELECT
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS RedCardMinutesStr
        FROM OPENJSON(player.value, '$.RedCardMinutes') AS j
    ) AS rcm

    OUTER APPLY (
        SELECT
            COUNT(*)                                         AS GoalsCount,
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS GoalsArrayStr
        FROM OPENJSON(player.value, '$.Goals') AS j
    ) AS g

    OUTER APPLY (
        SELECT
            COUNT(*)                                         AS AssistsCount,
            STRING_AGG(CONVERT(nvarchar(100), j.value), ',') AS AssistsArrayStr
        FROM OPENJSON(player.value, '$.Assists') AS j
    ) AS a
),

away_players AS (
    SELECT
        *,
        CASE
            WHEN STARTED_GAME = 1 AND WAS_SUBSTITUTED = 0 THEN 'Played Full Game'
            WHEN STARTED_GAME = 1 AND WAS_SUBSTITUTED = 1 THEN 'Played Subbed Off'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 1 AND WAS_SUBSTITUTED = 1 THEN 'Played Subbed On and Subbed Off'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 1 AND WAS_SUBSTITUTED = 0 THEN 'Played Subbed On'
            WHEN STARTED_GAME = 0 AND WAS_INTRODUCED = 0 THEN 'Did Not Play'
            ELSE 'Unknown Status'
        END AS PLAYER_STATUS
    FROM away_raw
)

SELECT * FROM home_players
UNION ALL
SELECT * FROM away_players;
