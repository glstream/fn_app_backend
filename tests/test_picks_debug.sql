-- Debug query to test base_picks CTE
SELECT  
    ft.owner_id as user_id,
    dp.year as season,
    dp.year,
    dp.year || ' ' || dp.round_name AS player_full_name,
    dp.roster_id,
    dp.owner_id,
    ft.team_id,
    ft.team_name,
    ft.owner_name
FROM dynastr.draft_picks dp
INNER JOIN dynastr.fleaflicker_teams ft 
    ON dp.owner_id = ft.team_id 
    AND dp.league_id = ft.league_id
    AND dp.session_id = ft.session_id
WHERE dp.league_id = '349505'
    AND dp.session_id = 'D4083299-2FDA-4C17-AA8E-2F8952D2F226'
    AND CAST(dp.round AS INTEGER) <= 4
    AND dp.owner_id = '1798697'
ORDER BY dp.year, dp.round, dp.roster_id;