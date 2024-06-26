WITH base_players as (SELECT
                    lp.user_id
                    , lp.league_id
                    , lp.session_id
                    , pl.player_id
                    , dd.name_id as fp_player_id
					, pl.full_name as player_full_name
                    , pl.player_position
                    , coalesce(dd.league_type, -1) as player_value
                    , RANK() OVER (PARTITION BY lp.user_id, pl.player_position ORDER BY coalesce(dd.league_type, -1) desc) as player_order
                    , qb_cnt
                    , rb_cnt
                    , wr_cnt
                    , te_cnt
                    , flex_cnt
                    , sf_cnt
                    , rf_cnt

                    FROM dynastr.league_players lp
                    INNER JOIN dynastr.players pl on lp.player_id = pl.player_id
                    LEFT JOIN dynastr.dd_player_ranks dd on lower(concat(pl.first_name,pl.last_name, pl.player_position)) = dd.name_id 
                    INNER JOIN dynastr.current_leagues cl on lp.league_id = cl.league_id and cl.session_id = 'session_id'
                    WHERE lp.session_id = 'session_id'
                    and lp.league_id = 'league_id'
                    and pl.player_position IN ('QB', 'RB', 'WR', 'TE' )
                    and dd.rank_type = 'rank_type'
                    )
     						   
                    , starters as (SELECT  
                    qb.user_id
                    , qb.player_id
                    , qb.fp_player_id
					, qb.player_full_name
                    , qb.player_position
                    , qb.player_position as fantasy_position
                    , qb.player_order
                    from base_players qb
                    where 1=1
                    and qb.player_position = 'QB'
                    and qb.player_order <= qb.qb_cnt
                    UNION ALL
                    SELECT 
                    rb.user_id
                    , rb.player_id
                    , rb.fp_player_id
					, rb.player_full_name
                    , rb.player_position
                    , rb.player_position as fantasy_position
                    , rb.player_order
                    from base_players rb
                    where 1=1
                    and rb.player_position = 'RB'
                    and rb.player_order <= rb.rb_cnt
                    UNION ALL
                    select 
                    wr.user_id
                    , wr.player_id
                    , wr.fp_player_id
					, wr.player_full_name
                    , wr.player_position
                    , wr.player_position as fantasy_position
                    , wr.player_order
                    from base_players wr
                    where wr.player_position = 'WR'
                    and wr.player_order <= wr.wr_cnt

                    UNION ALL
                    select 
                    te.user_id
                    , te.player_id
                    , te.fp_player_id
					, te.player_full_name
                    , te.player_position
                    , te.player_position as fantasy_position
                    , te.player_order
                    from 	
                    base_players te
                    where te.player_position = 'TE'
                    and te.player_order <= te.te_cnt
                    )

                    , flex as (
                    SELECT
                    ns.user_id
                    , ns.player_id
                    , ns.fp_player_id
					, ns.player_full_name
                    , ns.player_position
                    , 'FLEX' as fantasy_position
                    , ns.player_order
                    from (
                    SELECT
                    fp.user_id
                    , fp.fp_player_id
					, fp.player_full_name
                    , fp.player_id
                    , fp.player_position
                    , RANK() OVER (PARTITION BY fp.user_id ORDER BY fp.player_value desc) as player_order
                    , fp.flex_cnt
                    from base_players fp
                    left join starters s on s.fp_player_id = fp.fp_player_id
                    where 1=1
                    and s.fp_player_id IS NULL
                    and fp.player_position IN ('RB','WR','TE')  
                    order by player_order) ns
                    where player_order <= ns.flex_cnt)

                    ,super_flex as (
                    SELECT
                    ns_sf.user_id
                    , ns_sf.player_id
                    , ns_sf.fp_player_id
					, ns_sf.player_full_name
                    , ns_sf.player_position
                    , 'SUPER_FLEX' as fantasy_position
                    , ns_sf.player_order
                    from (
                    SELECT
                    fp.user_id
                    , fp.fp_player_id
					, fp.player_full_name
                    , fp.player_id
                    , fp.player_position
                    , RANK() OVER (PARTITION BY fp.user_id ORDER BY fp.player_value desc) as player_order
                    , fp.sf_cnt
                    from base_players fp
                    left join (select * from starters UNION ALL select * from flex) s on s.fp_player_id = fp.fp_player_id
                    where s.fp_player_id IS NULL
                    and fp.player_position IN ('QB','RB','WR','TE')  
                    order by player_order) ns_sf
                    where player_order <= ns_sf.sf_cnt)

                    ,rec_flex as (
                    SELECT
                    ns_rf.user_id
                    , ns_rf.player_id
                    , ns_rf.fp_player_id
                    , ns_rf.player_full_name
                    , ns_rf.player_position
                    , 'REC_FLEX' as fantasy_position
                    , ns_rf.player_order
                    from (
                    SELECT
                    fp.user_id
                    , fp.fp_player_id
                    , fp.player_id
                    , fp.player_full_name
                    , fp.player_position
                    , ROW_NUMBER() OVER (PARTITION BY fp.user_id ORDER BY fp.player_value desc) as player_order
                    , fp.rf_cnt
                    from base_players fp
                    left join (select * from starters UNION ALL select * from flex) s on s.fp_player_id = fp.fp_player_id
                    where s.fp_player_id IS NULL
                    and fp.player_position IN ('WR','TE')  
                    order by player_order) ns_rf
                    where player_order <= ns_rf.rf_cnt)

                    , all_starters as (select 
                    user_id
                    ,ap.player_id
                    ,ap.fp_player_id
					,ap.player_full_name
                    ,ap.player_position 
                    ,ap.fantasy_position
                    ,'STARTER' as fantasy_designation
                    ,ap.player_order
                    from (select * from starters UNION ALL select * from flex UNION ALL select * from super_flex UNION ALL select * from rec_flex) ap
                    order by user_id, player_position desc)
                                            
                    SELECT tp.user_id
                    ,m.display_name
                    ,coalesce(p.full_name, tp.player_full_name) as full_name
                    , tp.draft_year
                    ,lower(p.first_name) as first_name
					,lower(p.last_name) as last_name
                    ,p.team
                    ,p.age
                    ,tp.player_id as sleeper_id
                    ,tp.player_position
                    ,tp.fantasy_position
                    ,tp.fantasy_designation
                    ,coalesce(dd.league_type, -1) as player_value
                    ,coalesce(dd.sf_position_rank, -1) as player_rank
                    from (select 
                            user_id
                            ,ap.player_id
                            ,ap.fp_player_id
                            ,ap.player_full_name
                            , NULL as draft_year
                            ,ap.player_position 
                            ,ap.fantasy_position
                            ,'STARTER' as fantasy_designation
                            ,ap.player_order 
                            from all_starters ap
                            UNION
                            select 
                            bp.user_id
                            ,bp.player_id
                            ,bp.fp_player_id
                            ,bp.player_full_name
                            , NULL as draft_year
                            ,bp.player_position 
                            ,bp.player_position as fantasy_position
                            ,'BENCH' as fantasy_designation
                            ,bp.player_order
                            from base_players bp where bp.player_id not in (select player_id from all_starters)
                            UNION ALL
                            select 
                            user_id
                            ,null as player_id
                            ,picks.fp_player_id
                            ,CASE WHEN picks.season = picks.year THEN concat(picks.year, ' ', picks.round, '.', picks.position ) 
                            ELSE concat(picks.year,' Mid ', picks.round_name)
                            END AS player_full_name
                            ,picks.year as draft_year
                            ,'PICKS' as player_position 
                            ,'PICKS' as fantasy_position
                            ,'PICKS' as fantasy_designation
                            , null as player_order
                            from (SELECT t1.user_id
                                , t1.season
                                , t1.year
                                , t1.round
                                , t1.position
                                , t1.round_name 
                                , dd.name_id as fp_player_id
								, t1.player_full_name
								, coalesce(dd.league_type, -1)
                                FROM (
                                    SELECT  
                                    al.user_id
                                    , al.season
                                    , al.year
                                    , al.round 
                                    , al.round_name
                                    , dname.position
									 , CASE WHEN (dname.position::integer / al.leaguesize  < 0.33) and (al.draft_set_flg = 'Y') and (al.year = dname.season) THEN al.year || 'early' || al.round_name || 'pi'
                                            WHEN (dname.position::integer / al.leaguesize) >= 0.33 AND (dname.position::integer / al.leaguesize) <= 0.66 and al.draft_set_flg = 'Y' and al.year = dname.season  THEN al.year || 'mid' || al.round_name  || 'pi'
        								    WHEN (dname.position::integer / al.leaguesize) > 0.66 and al.draft_set_flg = 'Y' and al.year = dname.season THEN al.year || 'mid' || al.round_name  || 'pi'
        									ELSE al.year || 'mid' || al.round_name  || 'pi'
                                            END AS player_full_name 
                                        
                                    FROM (                           
                                        SELECT dp.roster_id
                                        , dp.year
                                        , dp.round_name
                                        , dp.round
                                        , dp.league_id
                                        , dpos.user_id
                                        , dpos.season
                                        , dpos.draft_set_flg
										, MAX(dpos.roster_id::integer) OVER () as leaguesize
                                        FROM dynastr.draft_picks dp
                                        INNER JOIN dynastr.draft_positions dpos on dp.owner_id = dpos.roster_id and dp.league_id = dpos.league_id

                                        WHERE dpos.league_id = 'league_id'
                                        and dp.session_id = 'session_id'
	
                                        ) al 
                                    INNER JOIN dynastr.draft_positions dname on  dname.roster_id = al.roster_id and al.league_id = dname.league_id

                                ) t1
                                LEFT JOIN dynastr.dd_player_ranks dd on t1.player_full_name = dd.name_id
                                where dd.rank_type = 'rank_type'
								) picks
                            ) tp
                    left join dynastr.players p on tp.player_id = p.player_id
                    LEFT JOIN dynastr.dd_player_ranks dd on tp.fp_player_id = dd.name_id
                    inner join dynastr.managers m on tp.user_id = m.user_id 
                    where 1=1
                    and dd.rank_type = 'rank_type'
                    order by player_value desc