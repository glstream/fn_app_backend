with ktc_players as (select player_full_name
,CONCAT(position, ' ', rank() OVER (partition by rank_type, position ORDER BY sf_value DESC)) as pos_rank
,  case when team = 'KCC' then 'KC' else team end as team
, case when round(CAST(age AS float)) < 1 then Null else round(CAST(age AS float)) end as age
, sf_value as value
, sf_rank as rank
, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN position = 'RDP' THEN 'Pick'
		ELSE position END as _position
, 'sf_value' as roster_type 
, rank_type
,insert_date
from dynastr.ktc_player_ranks 
UNION ALL
select player_full_name
,CONCAT(position, ' ', rank() OVER (partition by rank_type, position ORDER BY one_qb_value DESC)) as pos_rank
,  case when team = 'KCC' then 'KC' else team end as team
, case when round(CAST(age AS float)) < 1 then Null else round(CAST(age AS float)) end as age
, one_qb_value as value
, rank as rank
, CASE WHEN substring(lower(player_full_name) from 6 for 5) = 'round' THEN 'Pick' 
	   	WHEN position = 'RDP' THEN 'Pick'
		ELSE position END as _position
, 'one_qb_value' as roster_type
, rank_type
,insert_date
from dynastr.ktc_player_ranks )
															   
select
COALESCE(REPLACE(REPLACE(player_full_name, 'Round ', ''), ' Pick ', '.')) as player_full_name
, pos_rank
, team
, age
, value as player_value
, rank as player_rank
, row_number() OVER (order by value desc) as _rownum
, UPPER(_position) AS _position
, roster_type
, rank_type
, TO_DATE(insert_date, 'YYYY-mm-DDTH:M:SS.z')-1 as _insert_date
from ktc_players
where 1=1
and player_full_name not like '%2023%'
and value > 0
order by value desc
									 
