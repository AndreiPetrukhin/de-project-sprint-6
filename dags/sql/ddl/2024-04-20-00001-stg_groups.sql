-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00000-stg_groups.sql
-- Создание таблицы для групп
-- DROP TABLE IF EXISTS STV202404106__STAGING.groups;
CREATE TABLE IF NOT EXISTS STV202404106__STAGING.groups (
    id INT PRIMARY KEY,
    admin_id INT REFERENCES STV202404106__STAGING.users(id),
    group_name VARCHAR(100),
    registration_dt TIMESTAMP,
    is_private INT
)
order by 
	id, admin_id
PARTITION BY 
	registration_dt::date
GROUP BY 
	calendar_hierarchy_day(registration_dt::date, 3, 2);