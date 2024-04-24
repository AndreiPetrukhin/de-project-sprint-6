-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00004-dds_l_admins.sql
-- DROP TABLE IF EXISTS stv202404106__DWH.l_admins;

CREATE TABLE stv202404106__DWH.l_admins
(
    hk_l_admin_id BIGINT PRIMARY KEY,
    hk_user_id BIGINT NOT NULL CONSTRAINT fk_l_admins_user REFERENCES stv202404106__DWH.h_users (hk_user_id),
    hk_group_id BIGINT NOT NULL CONSTRAINT fk_l_admins_group REFERENCES stv202404106__DWH.h_groups (hk_group_id),
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY 
  load_dt
SEGMENTED BY 
  hk_l_admin_id ALL NODES
PARTITION BY 
  load_dt::date
GROUP BY 
  calendar_hierarchy_day(load_dt::date, 3, 2);
