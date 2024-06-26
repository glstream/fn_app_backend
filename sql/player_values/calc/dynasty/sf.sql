SELECT
  -- Format the pick number to always have two digits and include year
  CASE
    WHEN pre.player_full_name ~ '^[0-9]{4} Round [0-9]+ Pick [0-9]+$' THEN
      concat(
        substring(pre.player_full_name from '^[0-9]{4}'), -- Extracts the Year
        ' ',
        substring(pre.player_full_name from 'Round ([0-9]+) Pick [0-9]+$'), -- Extracts Round Number
        '.',
        lpad(substring(pre.player_full_name from 'Pick ([0-9]+)$'), 2, '0') -- Extracts Pick Number and pads it
      )
    ELSE
      pre.player_full_name
  END as player_full_name,
  pre.team,
  pre.age,
  pre._position,
  pre.sf_value,
  pre.sf_rank,
  pre.one_qb_value,
  pre.one_qb_rank,
  pre.insert_date,
  pre.player_id
FROM (
  SELECT
    sf.player_full_name,
    p.team,
    p.age,
    CASE
      WHEN substring(lower(sf.player_full_name) from 6 for 5) = 'round' THEN 'Pick'
      WHEN _position = 'RDP' THEN 'Pick'
      ELSE _position
    END as _position,
    sf.superflex_sf_value::int as sf_value,
    sf.superflex_sf_rank::int as sf_rank,
    sf.superflex_one_qb_value::int as one_qb_value,
    sf.superflex_one_qb_rank::int as one_qb_rank,
    sf.insert_date,
    sf.ktc_player_id as player_id
  FROM
    dynastr.sf_player_ranks sf
  LEFT JOIN dynastr.players p ON sf.player_full_name = p.full_name
  WHERE
    sf.player_full_name NOT LIKE '%2023%'
    AND (sf.superflex_sf_value > 0 OR sf.superflex_one_qb_value > 0)
    AND rank_type = 'dynasty'
) pre
ORDER BY
  pre.sf_value DESC;
