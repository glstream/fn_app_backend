select player_full_name
,  case when team = 'KCC' then 'KC' else team end as team
, age
, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN player_position = 'RDP' THEN 'Pick'
		ELSE player_position END as _position
, sf_value 
, sf_rank_ecr
, one_qb_value
, one_qb_rank_ecr
,insert_date
, fp_player_id as player_id
from dynastr.dp_player_ranks 
where 1=2
and player_full_name not like '%2023%'
and player_full_name not like '%2022%'
and (sf_value > 0 OR one_qb_value > 0)					 
order by sf_value desc