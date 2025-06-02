select 
    insert_date::date as value_date
    , superflex_sf_value as superflex_player_value
    , superflex_sf_rank as superflex_player_rank
    , max(superflex_sf_value) OVER (PARTITION BY ktc_player_id) as max_superflex_player_value
    , min(superflex_sf_value) OVER (PARTITION BY ktc_player_id) as min_superflex_player_value
    , max(superflex_sf_rank) OVER (PARTITION BY ktc_player_id) as max_superflex_player_rank
    , min(superflex_sf_rank) OVER (PARTITION BY ktc_player_id) as min_superflex_player_rank
    , superflex_one_qb_value as one_qb_player_value
    , superflex_one_qb_rank as one_qb_player_rank
    , max(superflex_one_qb_value) OVER (PARTITION BY ktc_player_id) as max_one_qb_player_value
    , min(superflex_one_qb_value) OVER (PARTITION BY ktc_player_id) as min_one_qb_player_value
    , max(superflex_one_qb_rank) OVER (PARTITION BY ktc_player_id) as max_one_qb_player_rank
    , min(superflex_one_qb_rank) OVER (PARTITION BY ktc_player_id) as min_one_qb_player_rank
 
from dynastr.sf_player_ranks_hist 
where 1=1
and ktc_player_id = 'player_id'
and rank_type = 'dynasty'
and insert_date::date between '2025-01-01' and '2025-12-31'
group by
insert_date::date
,ktc_player_id
, superflex_sf_value
, superflex_sf_rank
, superflex_one_qb_value
, superflex_one_qb_rank
order by insert_date::date asc