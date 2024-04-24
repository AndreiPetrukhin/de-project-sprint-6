-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00004-dds_l_user_message.sql
-- s_dialog_info satellite table
-- DROP TABLE IF EXISTS stv202404106__DWH.s_dialog_info;

CREATE TABLE stv202404106__DWH.s_dialog_info
(
  hk_message_id BIGINT NOT NULL CONSTRAINT fk_s_dialog_info_dialogs REFERENCES stv202404106__DWH.h_dialogs (hk_message_id),
  message VARCHAR(1000),
  message_from INT,
  message_to INT,
  load_dt DATETIME,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_message_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);