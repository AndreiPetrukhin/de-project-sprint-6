-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00004-dds_l_user_message.sql
-- DROP TABLE IF EXISTS stv202404106__DWH.l_user_message;

CREATE TABLE stv202404106__DWH.l_user_message
(
  hk_l_user_message BIGINT PRIMARY KEY,
  hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_user_message_user REFERENCES stv202404106__DWH.h_users (hk_user_id),
  hk_message_id BIGINT NOT NULL CONSTRAINT fk_l_user_message_message REFERENCES stv202404106__DWH.h_dialogs (hk_message_id),
  load_dt DATETIME,
  load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_user_id ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);
