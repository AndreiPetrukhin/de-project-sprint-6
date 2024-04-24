-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00000-stg_dialogs.sql
-- Создание таблицы для диалогов
--DROP TABLE IF EXISTS STV202404106__STAGING.dialogs;
CREATE TABLE IF NOT EXISTS STV202404106__STAGING.dialogs (
    message_id INT PRIMARY KEY,
    message_ts TIMESTAMP,
    message_from INT REFERENCES STV202404106.users(id),
    message_to INT REFERENCES STV202404106.users(id),  -- Может ссылаться и на пользователя, и на группу. Проверьте логику приложения.
    message VARCHAR(1000),
    message_group INT REFERENCES STV202404106.groups(id)  -- Опционально
)
ORDER BY 
	message_id
PARTITION BY message_ts::date
GROUP BY calendar_hierarchy_day(message_ts::date, 3, 2);