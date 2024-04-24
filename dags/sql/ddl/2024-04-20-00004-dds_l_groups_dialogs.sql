-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00003-dds_h_users.sql
-- DROP TABLE IF EXISTS stv202404106__DWH.l_groups_dialogs;

CREATE TABLE stv202404106__DWH.l_groups_dialogs
(
    hk_l_groups_dialogs BIGINT PRIMARY KEY,
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_groups_dialogs_group REFERENCES stv202404106__DWH.h_groups (hk_group_id),
    hk_message_id BIGINT NOT NULL CONSTRAINT fk_l_groups_dialogs_message REFERENCES stv202404106__DWH.h_dialogs (hk_message_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_l_groups_dialogs ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);
