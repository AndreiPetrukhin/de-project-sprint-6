INSERT INTO stv202404106__DWH.s_dialog_info(hk_message_id, message, message_from, message_to, load_dt, load_src)
SELECT
  hd.hk_message_id,
  d.message,
  d.message_from,
  d.message_to,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.dialogs AS d
LEFT JOIN stv202404106__DWH.h_dialogs AS hd ON d.message_id = hd.message_id;