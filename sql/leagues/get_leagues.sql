select ROW_NUMBER() OVER() as key
, session_id
, cl.user_name
,cl.user_id
, cl.league_id
, league_name
, avatar
, total_rosters
, qb_cnt
, CASE 
    WHEN sf_cnt > 0 THEN 'Superflex' 
    ELSE 'Single QB' END AS roster_type
, starter_cnt
, total_roster_cnt
, sport
, insert_date
, rf_cnt
, CASE 
    WHEN league_cat = 0 THEN 'Redraft' 
    WHEN league_cat = 1 THEN 'Keeper' 
    ELSE 'Dynasty' END AS league_type
, league_year
, rs.ktc_power_rank
, rs.sf_power_rank
, rs.fc_power_rank
, rs.dd_power_rank
, rs.dp_power_rank
, rs.ktc_starters_rank
, rs.sf_starters_rank
, rs.fc_starters_rank
, rs.dp_starters_rank
, rs.dd_starters_rank
, rs.ktc_bench_rank
, rs.sf_bench_rank
, rs.fc_bench_rank
, rs.dp_bench_rank
, rs.dd_bench_rank
, rs.ktc_picks_rank
, rs.sf_picks_rank
, rs.fc_picks_rank
, rs.dp_picks_rank
, rs.dd_picks_rank
, rs.espn_contender_rank
, rs.nfl_contender_rank
, rs.fp_contender_rank
, rs.fc_contender_rank
, rs.cbs_contender_rank

from dynastr.current_leagues cl
left join dynastr.ranks_summary rs on cl.league_id = rs.league_id and cl.user_id = rs.user_id
where 1=1
and session_id = 'session_id'
and cl.user_id ='user_id'
and league_year = 'league_year'