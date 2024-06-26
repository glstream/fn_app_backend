select COALESCE(REPLACE(REPLACE(player_full_name, 'Round ', ''), ' Pick ', ' ')) as player_full_name
, p.team					
, p.age
, fc.player_position as _position
, sf_value
, sf_overall_rank
, one_qb_value
, one_qb_overall_rank
,fc.insert_date
, fc_player_id as player_id
from dynastr.fc_player_ranks fc
left join dynastr.players p on fc.sleeper_player_id = p.player_id
where 1=1
and rank_type = 'dynasty'
and sf_value is not null		
and player_full_name not like '%2023%'
and (sf_value > 0 OR one_qb_value > 0)					 
order by sf_value desc