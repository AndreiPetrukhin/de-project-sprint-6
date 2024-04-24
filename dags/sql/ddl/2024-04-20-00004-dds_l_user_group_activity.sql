-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00004-dds_l_user_group_activity.sql
-- DROP TABLE IF EXISTS stv202404106__DWH.l_user_group_activity;

CREATE TABLE stv202404106__DWH.l_user_group_activity
(
    hk_l_user_group_activity INT PRIMARY KEY,  -- Основной ключ
    hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user_id REFERENCES stv202404106__DWH.h_users (hk_user_id),
    hk_group_id INT CONSTRAINT fk_l_user_group_activity_group_id REFERENCES stv202404106__DWH.h_groups (hk_group_id),
    load_dt DATETIME,                          -- Временная отметка о том, когда были загружены данные
    load_src VARCHAR(20)                       -- Данные об источнике
)
ORDER BY
    load_dt
SEGMENTED BY
    hash(hk_l_user_group_activity) ALL NODES
PARTITION BY
    load_dt::date
GROUP BY
    calendar_hierarchy_day(load_dt::date, 3, 2);
