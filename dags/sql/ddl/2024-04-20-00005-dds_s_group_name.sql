-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00005-dds_s_group_name.sql
-- s_group_name satellite table
--DROP TABLE IF EXISTS stv202404106__DWH.s_group_name;

CREATE TABLE stv202404106__DWH.s_group_name
(
  hk_group_id BIGINT NOT NULL CONSTRAINT fk_s_group_name_groups REFERENCES stv202404106__DWH.h_groups (hk_group_id),
  group_name VARCHAR(100),
  load_dt DATETIME,
  load_src VARCHAR(20)
)
ORDER BY load_dt
SEGMENTED BY hk_group_id ALL NODES
PARTITION BY load_dt::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);