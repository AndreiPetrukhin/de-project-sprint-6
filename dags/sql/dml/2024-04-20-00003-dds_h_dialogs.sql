-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00003-dds_h_dialogs.sql
INSERT INTO stv202404106__DWH.h_dialogs(hk_message_id, message_id, message_ts, load_dt, load_src)
SELECT
    hash(message_id) AS hk_message_id,
    message_id,
    message_ts,
    now() AS load_dt,
    's3' AS load_src
FROM STV202404106__STAGING.dialogs
WHERE hash(message_id) NOT IN (SELECT hk_message_id FROM stv202404106__DWH.h_dialogs);