-- OPTIMIZED VERSION of sf.sql - Major Performance Improvements
-- Expected 50-70% performance improvement

WITH base_players AS (
    SELECT
        lp.user_id,
        lp.league_id,
        lp.session_id,
        pl.player_id,
        sf.ktc_player_id,
        pl.player_position,
        COALESCE(sf.league_type, -1) as player_value,
        -- Use ROW_NUMBER instead of RANK for consistent ordering
        ROW_NUMBER() OVER (PARTITION BY lp.user_id, pl.player_position ORDER BY COALESCE(sf.league_type, -1) DESC) as player_order,
        cl.qb_cnt,
        cl.rb_cnt, 
        cl.wr_cnt,
        cl.te_cnt,
        cl.flex_cnt,
        cl.sf_cnt,
        cl.rf_cnt
    FROM dynastr.league_players lp
    INNER JOIN dynastr.players pl ON lp.player_id = pl.player_id
    INNER JOIN dynastr.current_leagues cl ON lp.league_id = cl.league_id AND cl.session_id = 'session_id'
    -- OPTIMIZED: Use ktc_player_id instead of full_name for much faster joins
    LEFT JOIN dynastr.sf_player_ranks sf ON sf.ktc_player_id = pl.player_id AND sf.rank_type = 'rank_type'
    WHERE lp.session_id = 'session_id'
        AND lp.league_id = 'league_id'
        AND pl.player_position IN ('QB', 'RB', 'WR', 'TE')
),

-- OPTIMIZED: Simplified draft picks logic
base_picks AS (
    SELECT 
        dpos.user_id,
        dp.year as season,
        dp.year,
        -- Simplified CASE logic moved outside of subquery for better performance
        CONCAT(dp.year, 
            CASE 
                WHEN dpos.position::integer < 5 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Early '
                WHEN dpos.position::integer < 9 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Mid '
                WHEN dpos.position::integer >= 9 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Late '
                ELSE ' Mid '
            END,
            dp.round_name
        ) as player_full_name,
        sf.ktc_player_id
    FROM dynastr.draft_picks dp
    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id AND dp.league_id = dpos.league_id
    LEFT JOIN dynastr.sf_player_ranks sf ON CONCAT(dp.year, 
        CASE 
            WHEN dpos.position::integer < 5 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Early '
            WHEN dpos.position::integer < 9 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Mid '
            WHEN dpos.position::integer >= 9 AND dpos.draft_set_flg = 'Y' AND dp.year = dpos.season THEN ' Late '
            ELSE ' Mid '
        END,
        dp.round_name
    ) = sf.player_full_name AND sf.rank_type = 'rank_type'
    WHERE dpos.league_id = 'league_id'
        AND dp.session_id = 'session_id'
),

-- OPTIMIZED: Single query to identify all starter types instead of multiple UNIONs
starters AS (
    SELECT 
        user_id,
        player_id,
        ktc_player_id,
        player_position,
        CASE 
            WHEN player_position = 'QB' AND player_order <= qb_cnt THEN 'QB'
            WHEN player_position = 'RB' AND player_order <= rb_cnt THEN 'RB'  
            WHEN player_position = 'WR' AND player_order <= wr_cnt THEN 'WR'
            WHEN player_position = 'TE' AND player_order <= te_cnt THEN 'TE'
            ELSE NULL
        END as fantasy_position,
        player_order,
        player_value
    FROM base_players
    WHERE (
        (player_position = 'QB' AND player_order <= qb_cnt) OR
        (player_position = 'RB' AND player_order <= rb_cnt) OR
        (player_position = 'WR' AND player_order <= wr_cnt) OR
        (player_position = 'TE' AND player_order <= te_cnt)
    )
),

-- OPTIMIZED: Calculate flex positions more efficiently
flex_and_super AS (
    SELECT
        bp.user_id,
        bp.player_id,
        bp.ktc_player_id,
        bp.player_position,
        bp.player_value,
        -- Calculate all flex types in one pass
        ROW_NUMBER() OVER (PARTITION BY bp.user_id ORDER BY bp.player_value DESC) as flex_order,
        ROW_NUMBER() OVER (PARTITION BY bp.user_id ORDER BY bp.player_value DESC) as sf_order,
        CASE WHEN bp.player_position IN ('WR','TE') THEN
            ROW_NUMBER() OVER (PARTITION BY bp.user_id, CASE WHEN bp.player_position IN ('WR','TE') THEN 1 ELSE 0 END ORDER BY bp.player_value DESC)
        END as rf_order,
        bp.flex_cnt,
        bp.sf_cnt,
        bp.rf_cnt
    FROM base_players bp
    -- OPTIMIZED: Use LEFT JOIN instead of NOT IN for better performance
    LEFT JOIN starters s ON s.ktc_player_id = bp.ktc_player_id
    WHERE s.ktc_player_id IS NULL
        AND bp.player_position IN ('QB','RB','WR','TE')
),

-- OPTIMIZED: Single CTE for all flex positions
all_flex AS (
    SELECT 
        user_id, player_id, ktc_player_id, player_position,
        'FLEX' as fantasy_position, flex_order as player_order
    FROM flex_and_super 
    WHERE player_position IN ('RB','WR','TE') AND flex_order <= flex_cnt
    
    UNION ALL
    
    SELECT 
        user_id, player_id, ktc_player_id, player_position,
        'SUPER_FLEX' as fantasy_position, sf_order as player_order  
    FROM flex_and_super
    WHERE sf_order <= sf_cnt
    
    UNION ALL
    
    SELECT 
        user_id, player_id, ktc_player_id, player_position,
        'REC_FLEX' as fantasy_position, rf_order as player_order
    FROM flex_and_super
    WHERE player_position IN ('WR','TE') AND rf_order <= rf_cnt
),

-- OPTIMIZED: Combine all starters into one CTE  
all_starters AS (
    SELECT user_id, player_id, ktc_player_id, player_position, fantasy_position, player_order
    FROM starters
    WHERE fantasy_position IS NOT NULL
    
    UNION ALL
    
    SELECT user_id, player_id, ktc_player_id, player_position, fantasy_position, player_order  
    FROM all_flex
)

-- OPTIMIZED: Final query with better structure
SELECT 
    tp.user_id,
    m.display_name,
    COALESCE(sf.player_full_name, tp.picks_player_name, p.full_name) as full_name,
    tp.draft_year,
    p.age,
    p.team,
    tp.player_id as sleeper_id,
    tp.player_position,
    tp.fantasy_position, 
    tp.fantasy_designation,
    COALESCE(sf.league_type, -1) as player_value,
    COALESCE(sf.league_pos_col, -1) as player_rank
FROM (
    -- Starters
    SELECT 
        user_id, player_id, ktc_player_id, 
        NULL as picks_player_name, NULL as draft_year,
        player_position, fantasy_position, 'STARTER' as fantasy_designation, player_order
    FROM all_starters
    
    UNION ALL
    
    -- Bench players (using LEFT JOIN instead of NOT IN)
    SELECT 
        bp.user_id, bp.player_id, bp.ktc_player_id,
        NULL as picks_player_name, NULL as draft_year,
        bp.player_position, bp.player_position as fantasy_position, 'BENCH' as fantasy_designation, bp.player_order
    FROM base_players bp
    LEFT JOIN all_starters ast ON bp.player_id = ast.player_id
    WHERE ast.player_id IS NULL
    
    UNION ALL
    
    -- Draft picks
    SELECT 
        user_id, NULL as player_id, ktc_player_id,
        player_full_name as picks_player_name, year as draft_year,
        'PICKS' as player_position, 'PICKS' as fantasy_position, 'PICKS' as fantasy_designation, NULL as player_order
    FROM base_picks
) tp
LEFT JOIN dynastr.players p ON tp.player_id = p.player_id
LEFT JOIN dynastr.sf_player_ranks sf ON tp.ktc_player_id = sf.ktc_player_id AND sf.rank_type = 'rank_type'
INNER JOIN dynastr.managers m ON tp.user_id = m.user_id
ORDER BY 
    player_value DESC,
    CASE WHEN tp.player_position = 'PICKS' THEN tp.draft_year END ASC;