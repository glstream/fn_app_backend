WITH league_config AS (
    SELECT qb_cnt, rb_cnt, wr_cnt, te_cnt, flex_cnt, sf_cnt, rf_cnt
    FROM dynastr.current_leagues 
    WHERE league_id = 'league_id' AND session_id = 'session_id'
),

player_rankings AS (
    SELECT 
        lp.user_id,
        pl.player_id,
        sf.ktc_player_id,
        pl.player_position,
        COALESCE(sf.league_type, -1) as player_value,
        p.age,
        p.team,
        ROW_NUMBER() OVER (PARTITION BY lp.user_id, pl.player_position ORDER BY COALESCE(sf.league_type, -1) DESC) as pos_rank,
        ROW_NUMBER() OVER (PARTITION BY lp.user_id ORDER BY COALESCE(sf.league_type, -1) DESC) as overall_rank,
        lc.qb_cnt, lc.rb_cnt, lc.wr_cnt, lc.te_cnt, lc.flex_cnt, lc.sf_cnt, lc.rf_cnt
    FROM dynastr.league_players lp
    INNER JOIN dynastr.players pl ON lp.player_id = pl.player_id
    INNER JOIN dynastr.players p ON pl.player_id = p.player_id
    LEFT JOIN dynastr.sf_player_ranks sf ON sf.player_full_name = pl.full_name AND sf.rank_type = 'rank_type'
    CROSS JOIN league_config lc
    WHERE lp.session_id = 'session_id' 
        AND lp.league_id = 'league_id'
        AND pl.player_position IN ('QB', 'RB', 'WR', 'TE')
),

draft_picks_ranked AS (
    SELECT 
        dpos.user_id,
        sf.ktc_player_id,
        'PICKS' as player_position,
        'PICKS' as fantasy_position,
        'PICKS' as fantasy_designation,
        COALESCE(sf.league_type, -1) as player_value,
        CASE 
            WHEN (dname.position::integer) < 13 AND dpos.draft_set_flg = 'Y' AND dp.year = dname.season
            THEN dp.year || ' Round ' || dp.round || ' Pick ' || dname.position
            WHEN (dname.position::integer) > 12 AND dpos.draft_set_flg = 'Y' AND dp.year = dname.season
            THEN dp.year || ' ' || dname.position_name || ' ' || dp.round_name 
            ELSE dp.year || ' Mid ' || dp.round_name 
        END AS player_full_name
    FROM dynastr.draft_picks dp
    INNER JOIN dynastr.draft_positions dpos ON dp.owner_id = dpos.roster_id AND dp.league_id = dpos.league_id
    INNER JOIN dynastr.draft_positions dname ON dname.roster_id = dp.roster_id AND dp.league_id = dname.league_id
    LEFT JOIN dynastr.sf_player_ranks sf ON sf.player_full_name = (
        CASE 
            WHEN (dname.position::integer) < 13 AND dpos.draft_set_flg = 'Y' AND dp.year = dname.season
            THEN dp.year || ' Round ' || dp.round || ' Pick ' || dname.position
            WHEN (dname.position::integer) > 12 AND dpos.draft_set_flg = 'Y' AND dp.year = dname.season
            THEN dp.year || ' ' || dname.position_name || ' ' || dp.round_name 
            ELSE dp.year || ' Mid ' || dp.round_name 
        END
    ) AND sf.rank_type = 'rank_type'
    WHERE dpos.league_id = 'league_id' 
        AND dp.session_id = 'session_id'
),

roster_assignments AS (
    SELECT 
        user_id,
        player_id,
        ktc_player_id,
        player_position,
        player_value,
        age,
        team,
        CASE 
            -- Position starters
            WHEN (player_position = 'QB' AND pos_rank <= qb_cnt) OR
                 (player_position = 'RB' AND pos_rank <= rb_cnt) OR
                 (player_position = 'WR' AND pos_rank <= wr_cnt) OR
                 (player_position = 'TE' AND pos_rank <= te_cnt)
            THEN player_position
            -- Flex positions (RB/WR/TE not already starting)
            WHEN player_position IN ('RB', 'WR', 'TE') AND 
                 pos_rank > CASE player_position 
                     WHEN 'RB' THEN rb_cnt 
                     WHEN 'WR' THEN wr_cnt 
                     WHEN 'TE' THEN te_cnt 
                 END AND
                 overall_rank <= (qb_cnt + rb_cnt + wr_cnt + te_cnt + flex_cnt)
            THEN 'FLEX'
            -- Super flex positions (any position not already assigned)
            WHEN overall_rank > (qb_cnt + rb_cnt + wr_cnt + te_cnt + flex_cnt) AND
                 overall_rank <= (qb_cnt + rb_cnt + wr_cnt + te_cnt + flex_cnt + sf_cnt)
            THEN 'SUPER_FLEX'
            ELSE player_position
        END as fantasy_position,
        CASE 
            WHEN overall_rank <= (qb_cnt + rb_cnt + wr_cnt + te_cnt + flex_cnt + sf_cnt)
            THEN 'STARTER'
            ELSE 'BENCH'
        END as fantasy_designation
    FROM player_rankings
),

base_data AS (
    -- Players
    SELECT
        ra.user_id,
        m.display_name,
        m.avatar,
        sf.player_full_name as full_name,
        ra.age,
        ra.team,
        ra.player_position,
        ra.fantasy_position,
        ra.fantasy_designation,
        ra.player_value
    FROM roster_assignments ra
    INNER JOIN dynastr.sf_player_ranks sf ON ra.ktc_player_id = sf.ktc_player_id AND sf.rank_type = 'rank_type'
    INNER JOIN dynastr.managers m ON ra.user_id = m.user_id
    
    UNION ALL
    
    -- Draft picks
    SELECT
        dp.user_id,
        m.display_name,
        m.avatar,
        dp.player_full_name as full_name,
        NULL as age,
        NULL as team,
        dp.player_position,
        dp.fantasy_position,
        dp.fantasy_designation,
        dp.player_value
    FROM draft_picks_ranked dp
    INNER JOIN dynastr.managers m ON dp.user_id = m.user_id
),

user_totals AS (
    SELECT 
        user_id,
        SUM(player_value) as total_value
    FROM base_data
    GROUP BY user_id
),

aggregated_data AS (
    SELECT
        bd.user_id,
        bd.display_name,
        bd.avatar,
        ut.total_value,
        -- QB metrics  
        SUM(CASE WHEN bd.player_position = 'QB' THEN bd.player_value ELSE 0 END) as qb_sum,
        SUM(CASE WHEN bd.player_position = 'QB' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value ELSE 0 END) as qb_starter_sum,
        SUM(CASE WHEN bd.player_position = 'QB' THEN bd.age ELSE 0 END) as qb_age_sum,
        SUM(CASE WHEN bd.player_position = 'QB' AND bd.fantasy_designation = 'STARTER' THEN bd.age ELSE 0 END) as qb_starter_age_sum,
        COUNT(CASE WHEN bd.player_position = 'QB' THEN 1 END) as qb_count,
        COUNT(CASE WHEN bd.player_position = 'QB' AND bd.fantasy_designation = 'STARTER' THEN 1 END) as qb_starter_count,
        MAX(CASE WHEN bd.player_position = 'QB' THEN bd.player_value END) as qb_value,
        MAX(CASE WHEN bd.player_position = 'QB' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value END) as qb_starter_value,
        -- RB metrics
        SUM(CASE WHEN bd.player_position = 'RB' THEN bd.player_value ELSE 0 END) as rb_sum,
        SUM(CASE WHEN bd.player_position = 'RB' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value ELSE 0 END) as rb_starter_sum,
        SUM(CASE WHEN bd.player_position = 'RB' THEN bd.age ELSE 0 END) as rb_age_sum,
        SUM(CASE WHEN bd.player_position = 'RB' AND bd.fantasy_designation = 'STARTER' THEN bd.age ELSE 0 END) as rb_starter_age_sum,
        COUNT(CASE WHEN bd.player_position = 'RB' THEN 1 END) as rb_count,
        COUNT(CASE WHEN bd.player_position = 'RB' AND bd.fantasy_designation = 'STARTER' THEN 1 END) as rb_starter_count,
        MAX(CASE WHEN bd.player_position = 'RB' THEN bd.player_value END) as rb_value,
        MAX(CASE WHEN bd.player_position = 'RB' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value END) as rb_starter_value,
        -- WR metrics
        SUM(CASE WHEN bd.player_position = 'WR' THEN bd.player_value ELSE 0 END) as wr_sum,
        SUM(CASE WHEN bd.player_position = 'WR' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value ELSE 0 END) as wr_starter_sum,
        SUM(CASE WHEN bd.player_position = 'WR' THEN bd.age ELSE 0 END) as wr_age_sum,
        SUM(CASE WHEN bd.player_position = 'WR' AND bd.fantasy_designation = 'STARTER' THEN bd.age ELSE 0 END) as wr_starter_age_sum,
        COUNT(CASE WHEN bd.player_position = 'WR' THEN 1 END) as wr_count,
        COUNT(CASE WHEN bd.player_position = 'WR' AND bd.fantasy_designation = 'STARTER' THEN 1 END) as wr_starter_count,
        MAX(CASE WHEN bd.player_position = 'WR' THEN bd.player_value END) as wr_value,
        MAX(CASE WHEN bd.player_position = 'WR' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value END) as wr_starter_value,
        -- TE metrics
        SUM(CASE WHEN bd.player_position = 'TE' THEN bd.player_value ELSE 0 END) as te_sum,
        SUM(CASE WHEN bd.player_position = 'TE' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value ELSE 0 END) as te_starter_sum,
        SUM(CASE WHEN bd.player_position = 'TE' THEN bd.age ELSE 0 END) as te_age_sum,
        SUM(CASE WHEN bd.player_position = 'TE' AND bd.fantasy_designation = 'STARTER' THEN bd.age ELSE 0 END) as te_starter_age_sum,
        COUNT(CASE WHEN bd.player_position = 'TE' THEN 1 END) as te_count,
        COUNT(CASE WHEN bd.player_position = 'TE' AND bd.fantasy_designation = 'STARTER' THEN 1 END) as te_starter_count,
        MAX(CASE WHEN bd.player_position = 'TE' THEN bd.player_value END) as te_value,
        MAX(CASE WHEN bd.player_position = 'TE' AND bd.fantasy_designation = 'STARTER' THEN bd.player_value END) as te_starter_value,
        -- Other metrics
        SUM(CASE WHEN bd.player_position = 'PICKS' THEN bd.player_value ELSE 0 END) as picks_sum,
        MAX(CASE WHEN bd.player_position = 'PICKS' THEN bd.player_value END) as picks_value,
        SUM(CASE WHEN bd.fantasy_position = 'FLEX' THEN bd.player_value ELSE 0 END) as flex_sum,
        MAX(CASE WHEN bd.fantasy_position = 'FLEX' THEN bd.player_value END) as flex_value,
        SUM(CASE WHEN bd.fantasy_position = 'SUPER_FLEX' THEN bd.player_value ELSE 0 END) as super_flex_sum,
        MAX(CASE WHEN bd.fantasy_position = 'SUPER_FLEX' THEN bd.player_value END) as super_flex_value,
        SUM(CASE WHEN bd.fantasy_designation = 'STARTER' THEN bd.player_value ELSE 0 END) as starters_sum,
        COUNT(CASE WHEN bd.fantasy_designation = 'STARTER' THEN 1 END) as starters_count,
        MAX(CASE WHEN bd.fantasy_designation = 'STARTER' THEN bd.player_value END) as starters_value,
        SUM(CASE WHEN bd.fantasy_designation = 'BENCH' THEN bd.player_value ELSE 0 END) as bench_sum,
        COUNT(CASE WHEN bd.fantasy_designation = 'BENCH' THEN 1 END) as bench_count,
        MAX(CASE WHEN bd.fantasy_designation = 'BENCH' THEN bd.player_value END) as bench_value
    FROM base_data bd
    INNER JOIN user_totals ut ON bd.user_id = ut.user_id
    GROUP BY bd.user_id, bd.display_name, bd.avatar, ut.total_value
)

SELECT
    user_id,
    display_name,
    avatar,
    total_value,
    ROW_NUMBER() OVER (ORDER BY total_value DESC) as total_rank,
    NTILE(10) OVER (ORDER BY total_value DESC) as total_tile,
    qb_value,
    qb_starter_value,
    RANK() OVER (ORDER BY qb_sum DESC) as qb_rank,
    RANK() OVER (ORDER BY qb_starter_sum DESC) as qb_starter_rank,
    NTILE(10) OVER (ORDER BY qb_sum DESC) as qb_tile,
    qb_sum,
    qb_starter_sum,
    COALESCE(ROUND(qb_sum / NULLIF(qb_count, 0), 0), 0) as qb_average_value,
    COALESCE(ROUND(qb_starter_sum / NULLIF(qb_starter_count, 0), 0), 0) as qb_starter_average_value,
    COALESCE(ROUND(qb_age_sum / NULLIF(qb_count, 0), 0), 0) as qb_average_age,
    COALESCE(ROUND(qb_starter_age_sum / NULLIF(qb_starter_count, 0), 0), 0) as qb_starter_average_age,
    qb_count,
    rb_value,
    rb_starter_value,
    RANK() OVER (ORDER BY rb_sum DESC) as rb_rank,
    RANK() OVER (ORDER BY rb_starter_sum DESC) as rb_starter_rank,
    NTILE(10) OVER (ORDER BY rb_sum DESC) as rb_tile,
    rb_sum,
    rb_starter_sum,
    COALESCE(ROUND(rb_sum / NULLIF(rb_count, 0), 0), 0) as rb_average_value,
    COALESCE(ROUND(rb_starter_sum / NULLIF(rb_starter_count, 0), 0), 0) as rb_starter_average_value,
    COALESCE(ROUND(rb_age_sum / NULLIF(rb_count, 0), 0), 0) as rb_average_age,
    COALESCE(ROUND(rb_starter_age_sum / NULLIF(rb_starter_count, 0), 0), 0) as rb_starter_average_age,
    rb_count,
    wr_value,
    wr_starter_value,
    RANK() OVER (ORDER BY wr_sum DESC) as wr_rank,
    RANK() OVER (ORDER BY wr_starter_sum DESC) as wr_starter_rank,
    NTILE(10) OVER (ORDER BY wr_sum DESC) as wr_tile,
    wr_sum,
    wr_starter_sum,
    COALESCE(ROUND(wr_sum / NULLIF(wr_count, 0), 0), 0) as wr_average_value,
    COALESCE(ROUND(wr_starter_sum / NULLIF(wr_starter_count, 0), 0), 0) as wr_starter_average_value,
    COALESCE(ROUND(wr_age_sum / NULLIF(wr_count, 0), 0), 0) as wr_average_age,
    COALESCE(ROUND(wr_starter_age_sum / NULLIF(wr_starter_count, 0), 0), 0) as wr_starter_average_age,
    wr_count,
    te_value,
    te_starter_value,
    RANK() OVER (ORDER BY te_sum DESC) as te_rank,
    RANK() OVER (ORDER BY te_starter_sum DESC) as te_starter_rank,
    NTILE(10) OVER (ORDER BY te_sum DESC) as te_tile,
    te_sum,
    te_starter_sum,
    COALESCE(ROUND(te_sum / NULLIF(te_count, 0), 0), 0) as te_average_value,
    COALESCE(ROUND(te_starter_sum / NULLIF(te_starter_count, 0), 0), 0) as te_starter_average_value,
    COALESCE(ROUND(te_age_sum / NULLIF(te_count, 0), 0), 0) as te_average_age,
    COALESCE(ROUND(te_starter_age_sum / NULLIF(te_starter_count, 0), 0), 0) as te_starter_average_age,
    te_count,
    picks_value,
    RANK() OVER (ORDER BY picks_sum DESC) as picks_rank,
    NTILE(10) OVER (ORDER BY picks_sum DESC) as picks_tile,
    picks_sum,
    flex_value,
    RANK() OVER (ORDER BY flex_sum DESC) as flex_rank,
    super_flex_value,
    RANK() OVER (ORDER BY super_flex_sum DESC) as super_flex_rank,
    starters_value,
    RANK() OVER (ORDER BY starters_sum DESC) as starters_rank,
    NTILE(10) OVER (ORDER BY starters_sum DESC) as starters_tile,
    starters_sum,
    COALESCE(ROUND(starters_sum / NULLIF(starters_count, 0), 0), 0) as starters_average,
    starters_count,
    bench_value,
    RANK() OVER (ORDER BY bench_sum DESC) as bench_rank,
    NTILE(10) OVER (ORDER BY bench_sum DESC) as bench_tile,
    bench_sum,
    COALESCE(ROUND(bench_sum / NULLIF(bench_count, 0), 0), 0) as bench_average,
    bench_count
FROM aggregated_data
ORDER BY total_value DESC;