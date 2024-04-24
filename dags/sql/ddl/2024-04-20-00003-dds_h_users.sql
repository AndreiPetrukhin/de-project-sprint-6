-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00003-dds_h_users.sql
--drop table if exists stv202404106__DWH.h_users;

create table stv202404106__DWH.h_users
(
    hk_user_id bigint primary key,
    user_id      int,
    registration_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order 
    by load_dt
SEGMENTED 
    BY hk_user_id all nodes
PARTITION 
    BY load_dt::date
GROUP 
    BY calendar_hierarchy_day(load_dt::date, 3, 2);
