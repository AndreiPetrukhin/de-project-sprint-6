-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00004-dds_l_user_message.sql
INSERT INTO stv202404106__DWH.l_user_message (hk_l_user_message, hk_user_id, hk_message_id, load_dt, load_src)
SELECT
  HASH(hu.hk_user_id, hd.hk_message_id) AS hk_l_user_message,
  hu.hk_user_id,
  hd.hk_message_id,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.dialogs AS d
LEFT JOIN stv202404106__DWH.h_users AS hu ON d.message_from = hu.user_id
LEFT JOIN stv202404106__DWH.h_dialogs AS hd ON d.message_id = hd.message_id
WHERE HASH(hu.hk_user_id, hd.hk_message_id) NOT IN (SELECT hk_l_user_message FROM stv202404106__DWH.l_user_message);