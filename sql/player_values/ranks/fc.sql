
with fc_players as (select player_full_name
,CONCAT(fc.player_position, ' ', rank() OVER (partition by rank_type, fc.player_position ORDER BY sf_value DESC)) as pos_rank
, p.team					
, p.age
, sf_value as value
, fc.player_position as _position
, 'sf_value' as roster_type 
, rank_type
,fc.insert_date
from dynastr.fc_player_ranks fc
left join dynastr.players p on fc.sleeper_player_id = p.player_id
and sf_value is not null					
UNION ALL
select player_full_name
,CONCAT(fc.player_position, ' ', rank() OVER (partition by rank_type, fc.player_position ORDER BY one_qb_value DESC)) as pos_rank
, p.team					
, p.age
, one_qb_value as value
, fc.player_position as _position
, 'one_qb_value' as roster_type
, rank_type 
,fc.insert_date
from dynastr.fc_player_ranks fc 
left join dynastr.players p on fc.sleeper_player_id = p.player_id 				
where 1=1
and one_qb_value is not null 					
)
															   
select
COALESCE(REPLACE(REPLACE(player_full_name, 'Round ', ''), ' Pick ', ' ')) as player_full_name
,player_full_name
, pos_rank
, team
, age
, value as player_value
, row_number() OVER (partition by rank_type order by value desc) as player_rank
, UPPER(_position) AS _position
, roster_type
, rank_type
, TO_DATE(insert_date, 'YYYY-mm-DDTH:M:SS.z')-1 as _insert_date
from fc_players
where 1=1
and player_full_name not like '%2023%'
and value > 0