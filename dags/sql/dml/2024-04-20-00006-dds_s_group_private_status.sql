INSERT INTO stv202404106__DWH.s_group_private_status(hk_group_id, is_private, load_dt, load_src)
SELECT
  hg.hk_group_id,
  CASE WHEN g.is_private = 1 THEN TRUE ELSE FALSE END AS is_private,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.groups AS g
LEFT JOIN stv202404106__DWH.h_groups AS hg ON g.id = hg.group_id;