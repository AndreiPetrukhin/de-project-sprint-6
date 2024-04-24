INSERT INTO stv202404106__DWH.s_group_name(hk_group_id, group_name, load_dt, load_src)
SELECT
  hg.hk_group_id,
  g.group_name,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.groups AS g
LEFT JOIN stv202404106__DWH.h_groups AS hg ON g.id = hg.group_id;