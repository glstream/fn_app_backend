with dp_players as (select player_full_name
,  case when team = 'KCC' then 'KC' else team end as team
, age
, sf_value as value

, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN player_position = 'RDP' THEN 'Pick'
		ELSE player_position END as _position
, 'sf_value' as _rank_type 
,insert_date
from dynastr.dp_player_ranks 
UNION ALL
select player_full_name
,  case when team = 'KCC' then 'KC' else team end as team
, age
, one_qb_value as value
, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN player_position = 'RDP' THEN 'Pick'
		ELSE player_position END as _position
, 'one_qb_value' as _rank_type
,insert_date
from dynastr.dp_player_ranks )
select player_full_name
,CONCAT(_position, ' ', rank() OVER (partition by _rank_type, _position ORDER BY value DESC)) as pos_rank
, team
, age
, value as player_value
, row_number() OVER (partition by _rank_type order by value desc) as player_rank
, _position
, _rank_type
, 'DynastyProcess'
, TO_DATE(insert_date, 'YYYY-mm-DDTH:M:SS.z')-1 as _insert_date
from dp_players
where 1=1
and player_full_name not like '%2023%'
and value > 0
order by value desc

UNION ALL 


with fc_players as (select player_full_name
, p.team					
, p.age
, sf_value as value
, fc.player_position as _position
, 'sf_value' as _rank_type 
,fc.insert_date
from dynastr.fc_player_ranks fc
left join dynastr.players p on fc.sleeper_player_id = p.player_id
where 1=1
and rank_type = 'dynasty'
and sf_value is not null					
UNION ALL
select player_full_name
, p.team					
, p.age
, one_qb_value as value
, fc.player_position as _position
, 'one_qb_value' as _rank_type 
,fc.insert_date
from dynastr.fc_player_ranks fc 
left join dynastr.players p on fc.sleeper_player_id = p.player_id 				
where 1=1
and rank_type = 'dynasty'
and one_qb_value is not null 					
)
															   
select player_full_name
,CONCAT(_position, ' ', rank() OVER (partition by _rank_type, _position ORDER BY value DESC)) as pos_rank
, team
, age
, value as player_value
, row_number() OVER (partition by _rank_type order by value desc) as player_rank
, _position
, _rank_type
, 'FantasyValc'
, TO_DATE(insert_date, 'YYYY-mm-DDTH:M:SS.z')-1 as _insert_date
from fc_players
where 1=1
and player_full_name not like '%2023%'
and value > 0
UNION ALL
select player_full_name
,  case when team = 'KCC' then 'KC' else team end as team
, case when round(CAST(age AS float)) < 1 then Null else round(CAST(age AS float)) end as age
, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN position = 'RDP' THEN 'Pick'
		ELSE position END as _position
, sf_value 
, sf_rank 
, one_qb_value
, rank as one_qb_rank
,insert_date
from dynastr.ktc_player_ranks
where 1=1
and player_full_name not like '%2023%'
and (sf_value > 0 OR one_qb_value > 0)					 
order by sf_value desc