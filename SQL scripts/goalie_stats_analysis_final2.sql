-- Auto Generated (Do not modify) D4FBB274D4F68B463AA46A91EA156451650E163EF42DC56E208AECC53476F7F8
CREATE VIEW [dbo].[goalie_stats_analysis_final2] AS

--WITH game_player_teams AS (
    --SELECT game_id, player_id, team_id
    --FROM game_skater_stats
    --GROUP BY game_id, player_id, team_id
    --UNION
    --SELECT game_id, player_id, team_id
    --FROM game_goalie_stats
    --GROUP BY game_id, player_id, team_id
--),

-- 🟦 RAW per-game goalie data
WITH raw_stats AS (
    SELECT 
        ggs.game_id,
        ggs.player_id,
        --p.first_name + ' ' + p.last_name AS player_name,
        ggs.team_id,
        --t.teamName,
        g.season,
        ggs.shots,
        ggs.saves,
        ggs.shots - ggs.saves AS goals,
        ggs.powerPlaySaves,
        ggs.powerPlayShotsAgainst,
        ggs.evenSaves,
        ggs.evenShotsAgainst,
        ggs.shortHandedSaves,
        ggs.shortHandedShotsAgainst,
        ggs.timeOnIce,
        ROW_NUMBER() OVER (
            PARTITION BY ggs.player_id, ggs.game_id 
            ORDER BY ggs.timeOnIce DESC
        ) AS rn
    FROM game_goalie_stats AS ggs
    --JOIN player_info AS p ON p.player_id = ggs.player_id
    --JOIN team_info AS t ON t.team_id = ggs.team_id
    JOIN game AS g ON g.game_id = ggs.game_id
    JOIN game_player_team gpt ON gpt.player_id = ggs.player_id AND gpt.game_id = ggs.game_id AND gpt.team_id = ggs.team_id
    --WHERE EXISTS (
        --SELECT 1
        --FROM game_player_teams gpt
        --WHERE gpt.game_id = ggs.game_id AND gpt.player_id = ggs.player_id
    --)
),

-- 🔁 Deduplicated raw stats
deduped_raw_stats AS (
    SELECT *
    FROM raw_stats
    WHERE rn = 1
),

-- 🟩 AGGREGATED player stats by season
agg_stats AS (
    SELECT 
        player_id,
        season,
        --teamName,
        team_id,
        COUNT(DISTINCT game_id) AS total_games,
        SUM(shots) AS total_shots,
        SUM(saves) AS total_saves,
        SUM(goals) AS total_goals,
        SUM(powerPlaySaves) AS pp_saves,
        SUM(powerPlayShotsAgainst) AS pp_shots,
        SUM(evenSaves) AS ev_saves,
        SUM(evenShotsAgainst) AS ev_shots,
        SUM(shortHandedSaves) AS sh_saves,
        SUM(shortHandedShotsAgainst) AS sh_shots,
        SUM(timeOnIce) AS total_toi_seconds,
        
        ROUND(SUM(powerPlaySaves) * 100.0 / NULLIF(SUM(powerPlayShotsAgainst), 0), 2) AS pp_save_pct,
        ROUND(SUM(evenSaves) * 100.0 / NULLIF(SUM(evenShotsAgainst), 0), 2) AS ev_save_pct,
        ROUND(SUM(shortHandedSaves) * 100.0 / NULLIF(SUM(shortHandedShotsAgainst), 0), 2) AS sh_save_pct,
        ROUND(SUM(saves) * 100.0 / NULLIF(SUM(shots), 0), 2) AS overall_save_pct,
        ROUND(SUM(saves) * 1.0 / NULLIF(COUNT(DISTINCT game_id), 0), 2) AS avg_saves_per_game,
        ROUND((SUM(goals) * 60.0) / NULLIF(SUM(timeOnIce) / 60.0, 0), 2) AS gaa
    FROM deduped_raw_stats
    GROUP BY player_id, season, team_id --teamName
),

raw_game_counted AS (
    SELECT 
        player_id,
        season,
        --teamName,
        team_id,
        COUNT(DISTINCT game_id) AS raw_game_count
    FROM deduped_raw_stats
    GROUP BY player_id, season, team_id --teamName,
)

-- 🔄 FINAL SELECT
SELECT 
    a.player_id,
    --r.player_name,
    a.team_id,
    --a.teamName,
    a.season,
    a.total_games,
    a.total_shots,
    a.total_saves,
    a.total_goals,
    a.pp_saves,
    a.pp_shots,
    a.ev_saves,
    a.ev_shots,
    a.sh_saves,
    a.sh_shots,
    a.total_toi_seconds,
    a.pp_save_pct,
    a.ev_save_pct,
    a.sh_save_pct,
    a.overall_save_pct,
    a.avg_saves_per_game,
    a.gaa,

    r.game_id AS raw_game_id,
    r.shots AS raw_shots,
    r.saves AS raw_saves,
    r.goals AS raw_goals,
    r.powerPlaySaves AS raw_pp_saves,
    r.powerPlayShotsAgainst AS raw_pp_shots,
    r.evenSaves AS raw_ev_saves,
    r.evenShotsAgainst AS raw_ev_shots,
    r.shortHandedSaves AS raw_sh_saves,
    r.shortHandedShotsAgainst AS raw_sh_shots,
    r.timeOnIce AS raw_time_on_ice,

    rgc.raw_game_count

FROM agg_stats a
JOIN deduped_raw_stats r
    ON a.player_id = r.player_id
    AND a.season = r.season
    --AND a.teamName = r.teamName
    AND a.team_id = r.team_id
JOIN raw_game_counted rgc
    ON a.player_id = rgc.player_id
    AND a.season = rgc.season
    --AND a.teamName = rgc.teamName;
    AND a.team_id = rgc.team_id;