-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00003-dds_h_groups.sql
-- Удаление таблицы h_groups, если она уже существует
DROP TABLE IF EXISTS stv202404106__DWH.h_groups;

-- Создание таблицы h_groups
CREATE TABLE stv202404106__DWH.h_groups
(
    hk_group_id bigint PRIMARY KEY,
    group_id int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
ORDER 
    BY load_dt
SEGMENTED 
    BY hk_group_id ALL NODES
PARTITION 
    BY load_dt::date
GROUP 
    BY calendar_hierarchy_day(load_dt::date, 3, 2);