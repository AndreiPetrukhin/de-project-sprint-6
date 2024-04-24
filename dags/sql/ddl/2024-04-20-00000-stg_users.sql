-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00000-stg_users.sql
-- Создание таблицы для пользователей
-- DROP TABLE IF EXISTS STV202404106__STAGING.users CASCADE;
CREATE TABLE IF NOT EXISTS STV202404106__STAGING.users (
    id INT PRIMARY KEY,
    chat_name VARCHAR(200),
    registration_dt TIMESTAMP,
    country VARCHAR(200),
    age INT
)
ORDER BY 
	id;