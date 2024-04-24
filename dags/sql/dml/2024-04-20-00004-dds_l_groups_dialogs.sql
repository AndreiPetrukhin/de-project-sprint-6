-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00004-dds_l_groups_dialogs.sql
INSERT INTO stv202404106__DWH.l_groups_dialogs (hk_l_groups_dialogs, hk_group_id, hk_message_id, load_dt, load_src)
SELECT
  HASH(hg.hk_group_id, hd.hk_message_id) AS hk_l_groups_dialogs,
  hg.hk_group_id,
  hd.hk_message_id,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.dialogs AS d
LEFT JOIN stv202404106__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
LEFT JOIN stv202404106__DWH.h_groups AS hg ON d.message_group = hg.group_id
WHERE HASH(hg.hk_group_id, hd.hk_message_id) NOT IN (SELECT hk_l_groups_dialogs FROM stv202404106__DWH.l_groups_dialogs)
AND hg.hk_group_id IS NOT NULL;