-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00003-dds_h_dialogs.sql
-- Удаление таблицы h_dialogs, если она уже существует
DROP TABLE IF EXISTS stv202404106__DWH.h_dialogs;

-- Создание таблицы h_dialogs
CREATE TABLE stv202404106__DWH.h_dialogs
(
    hk_message_id bigint PRIMARY KEY,
    message_id int,
    message_ts datetime,
    load_dt datetime,
    load_src varchar(20)
)
ORDER 
    BY load_dt
SEGMENTED 
    BY hk_message_id ALL NODES
PARTITION 
    BY load_dt::date
GROUP 
    BY calendar_hierarchy_day(load_dt::date, 3, 2);
