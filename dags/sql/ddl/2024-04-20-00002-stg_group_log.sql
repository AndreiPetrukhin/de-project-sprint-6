-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/ddl/2024-04-20-00000-stg_dialogs.sql
-- DROP TABLE IF EXISTS STV202404106__STAGING.group_log;

CREATE TABLE IF NOT EXISTS STV202404106__STAGING.group_log (
    event_id AUTO_INCREMENT PRIMARY KEY,
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(50),
    event_datetime TIMESTAMP,

    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES STV202404106__STAGING.users(id),
    CONSTRAINT fk_user_id_from FOREIGN KEY (user_id_from) REFERENCES STV202404106__STAGING.users(id),
    CONSTRAINT fk_group_id FOREIGN KEY (group_id) REFERENCES STV202404106__STAGING.groups(id)
)
ORDER BY 
    event_id
SEGMENTED BY 
    hash(group_id, user_id) ALL NODES
PARTITION BY 
    event_datetime::date
GROUP BY 
	calendar_hierarchy_day(event_datetime::date, 3, 2);
