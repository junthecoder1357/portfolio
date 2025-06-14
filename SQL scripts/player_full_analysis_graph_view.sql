-- Auto Generated (Do not modify) A577E3F30FB0DF2B55E7BFD23BFF342C4817241A23EBF4F61222A9C7CA3B1A30
CREATE VIEW [dbo].[player_full_analysis_graph_view] AS

WITH 
-- STEP 1: Season-level stats
season_stats AS (
    SELECT
        p.player_id,
        p.first_name + ' ' + p.last_name AS player_name,
        p.primary_position,
        g.season,
        gpt.team_id,
        t.teamName,
        COUNT(DISTINCT gss.game_id) AS total_games,
        SUM(gss.goals) AS total_goals,
        SUM(gss.assists) AS total_assists,
        SUM(gss.goals + gss.assists) AS total_points,
        SUM(gss.powerPlayGoals) AS total_powerplay_goals,
        SUM(gss.powerPlayAssists) AS total_powerplay_assists,
        SUM(gss.shortHandedGoals) AS total_shorthanded_goals,
        SUM(gss.plusMinus) AS total_plusminus,
        SUM(gss.timeOnIce) AS total_time_on_ice
    FROM game_skater_stats gss
    JOIN player_info p ON p.player_id = gss.player_id
    JOIN game g ON g.game_id = gss.game_id
    JOIN game_player_team gpt ON gpt.player_id = gss.player_id 
        AND gpt.game_id = gss.game_id 
        AND gpt.team_id = gss.team_id
    JOIN team_info t ON t.team_id = gpt.team_id
    GROUP BY p.player_id, p.first_name, p.last_name, p.primary_position, 
             g.season, gpt.team_id, t.teamName
),

-- STEP 2: Rank players for top 5 selection
season_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY primary_position, season
            ORDER BY total_points DESC, total_goals DESC
        ) AS position_rank
    FROM season_stats
),

season_distinct AS (
    SELECT DISTINCT
        player_id,
        player_name,
        primary_position,
        season,
        team_id,
        teamName,
        position_rank,
        total_games,
        total_goals,
        total_assists,
        total_points,
        total_powerplay_goals,
        total_shorthanded_goals,
        total_plusminus,
        total_time_on_ice
    FROM season_ranked
),

ranked_players AS (
    SELECT DISTINCT player_id, primary_position, season, position_rank
    FROM season_ranked
),

GoalScorers AS (
    SELECT 
        gp.game_id,
        gp.play_id,
        CAST(gp.period AS VARCHAR) AS period,
        gpp.player_id,
        ROW_NUMBER() OVER (
            PARTITION BY gp.game_id, gp.play_id, gpp.player_id
            ORDER BY gp.play_id
        ) AS rn
    FROM game_plays gp
    INNER JOIN game_plays_players gpp 
        ON gp.play_id = gpp.play_id AND gp.game_id = gpp.game_id
    WHERE gp.event = 'Goal' AND gpp.playerType = 'Scorer'
),

PeriodGoals AS (
    SELECT 
        gs.player_id,
        gs.game_id,
        CAST(gs.period AS VARCHAR) AS period,
        COUNT(*) AS goals_in_period
    FROM GoalScorers gs
    WHERE gs.rn = 1
    GROUP BY gs.player_id, gs.game_id, gs.period
),

ShiftAnalysis AS (
    SELECT 
        rp.player_id,
        rp.primary_position,
        rp.season,
        gsh.game_id,
        CAST(gsh.period AS VARCHAR) AS period,
        COUNT(*) AS shift_count,
        SUM(CAST(gsh.shift_end AS FLOAT) - CAST(gsh.shift_start AS FLOAT)) / 60 AS total_shift_minutes,
        AVG(CAST(gsh.shift_end AS FLOAT) - CAST(gsh.shift_start AS FLOAT)) / 60 AS avg_shift_length
    FROM ranked_players rp
    INNER JOIN game_shifts gsh ON rp.player_id = gsh.player_id
    INNER JOIN game g ON gsh.game_id = g.game_id AND g.season = rp.season
    GROUP BY rp.player_id, rp.primary_position, rp.season, gsh.game_id, gsh.period
),

FatigueMetrics AS (
    SELECT 
        sa.*,
        sa.avg_shift_length - FIRST_VALUE(sa.avg_shift_length) 
            OVER (PARTITION BY sa.player_id, sa.game_id ORDER BY sa.period) AS shift_length_decline,
        sa.shift_count - FIRST_VALUE(sa.shift_count) 
            OVER (PARTITION BY sa.player_id, sa.game_id ORDER BY sa.period) AS shift_frequency_decline,
        sa.avg_shift_length - LAG(sa.avg_shift_length) 
            OVER (PARTITION BY sa.player_id, sa.game_id ORDER BY sa.period) AS period_shift_change
    FROM ShiftAnalysis sa
),

FatigueAgg AS (
    SELECT
        player_id,
        season,
        COUNT(DISTINCT game_id) AS games_tracked,
        SUM(shift_count) AS total_shifts,
        SUM(total_shift_minutes) AS total_minutes,
        AVG(avg_shift_length) AS avg_shift_length_overall,
        AVG(shift_length_decline) AS avg_shift_length_decline,
        AVG(shift_frequency_decline) AS avg_shift_freq_decline
    FROM FatigueMetrics
    GROUP BY player_id, season
),

PeriodGoalsDetail AS (
    SELECT 
        pg.player_id,
        g.season,
        pg.period,
        SUM(pg.goals_in_period) AS goals_in_period
    FROM PeriodGoals pg
    JOIN game g ON g.game_id = pg.game_id
    GROUP BY pg.player_id, g.season, pg.period
),

final_data AS (
    SELECT 
        sd.player_id,
        sa.game_id,
        sd.player_name,
        sd.teamName,
        sd.season,
        sd.primary_position,
        sd.position_rank,
        sd.total_games,
        sd.total_goals,
        sd.total_assists,
        sd.total_points,
        sd.total_powerplay_goals,
        sd.total_shorthanded_goals,
        sd.total_plusminus,
        sd.total_time_on_ice,
        fa.games_tracked,
        fa.total_shifts,
        fa.total_minutes,
        fa.avg_shift_length_overall,
        fa.avg_shift_length_decline,
        fa.avg_shift_freq_decline,
        sa.period,
        COALESCE(pgd.goals_in_period, 0) AS total_goals_in_period
    FROM season_distinct sd
    LEFT JOIN FatigueAgg fa 
        ON sd.player_id = fa.player_id AND sd.season = fa.season
    INNER JOIN ShiftAnalysis sa 
    ON sd.player_id = sa.player_id 
    AND sd.season = sa.season
    --AND sa.game_id = sa.game_id -- join on game_id (make sure the source has game_id)
    LEFT JOIN PeriodGoalsDetail pgd 
        ON sa.player_id = pgd.player_id 
        AND sa.season = pgd.season 
        AND sa.period = pgd.period
)

SELECT
    player_id,
    game_id,
    player_name,
    teamName,
    season,
    period,
    primary_position,
    position_rank,
    total_games,
    total_goals,
    total_assists,
    total_points,
    total_powerplay_goals,
    total_shorthanded_goals,
    total_plusminus,
    total_time_on_ice,
    games_tracked,
    total_shifts,
    total_minutes,
    avg_shift_length_overall,
    avg_shift_length_decline,
    avg_shift_freq_decline,
    total_goals_in_period
FROM final_data
-- Uncomment if needed:
-- ORDER BY season, primary_position, position_rank, period;