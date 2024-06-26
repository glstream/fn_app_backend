with dp_players as (select player_full_name
,  case when team = 'KCC' then 'KC' else team end as team
, age
, sf_value as value

, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN player_position = 'RDP' THEN 'Pick'
		ELSE player_position END as _position
, 'sf_value' as roster_type 
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
, 'one_qb_value' as roster_type
,insert_date
from dynastr.dp_player_ranks )
select 
COALESCE(REPLACE(REPLACE(player_full_name, 'Round ', ''), ' Pick ', '.')) as player_full_name
,CONCAT(_position, ' ', rank() OVER (partition by roster_type, _position ORDER BY value DESC)) as pos_rank
, team
, age
, value as player_value
, row_number() OVER (partition by roster_type order by value desc) as player_rank
, UPPER(_position) AS _position
, roster_type
, 'dynasty' as rank_type
, TO_DATE(insert_date, 'YYYY-mm-DDTH:M:SS.z')-1 as _insert_date
from dp_players
where 1=1
and player_full_name not like '%2023%' and player_full_name not like '%2022%'
and value > 0
order by value desc