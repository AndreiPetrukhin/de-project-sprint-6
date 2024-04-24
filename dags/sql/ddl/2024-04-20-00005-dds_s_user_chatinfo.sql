-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00004-dds_l_user_message.sql
-- Drop the satellite table if it already exists
-- DROP TABLE IF EXISTS stv202404106__DWH.s_user_chatinfo;

-- Create the new satellite table
CREATE TABLE stv202404106__DWH.s_user_chatinfo
(
  hk_user_id BIGINT NOT NULL CONSTRAINT fk_s_user_chatinfo_users REFERENCES stv202404106__DWH.h_users (hk_user_id),
  chat_name VARCHAR(200),
  load_dt DATETIME,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_user_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);

