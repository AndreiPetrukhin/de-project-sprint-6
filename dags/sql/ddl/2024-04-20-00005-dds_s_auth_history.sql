-- Удаление таблицы s_auth_history, если она уже существует
-- DROP TABLE IF EXISTS stv202404106__DWH.s_auth_history;

-- Создание таблицы s_auth_history
CREATE TABLE stv202404106__DWH.s_auth_history
(
    hk_l_user_group_activity INT,
    user_id_from INT,
    event VARCHAR(50),
    event_dt DATETIME,
    load_dt DATETIME,
    load_src VARCHAR(20)
)
ORDER BY
    load_dt
SEGMENTED BY
    hk_l_user_group_activity ALL NODES
PARTITION BY
    load_dt::DATE
GROUP BY
    CALENDAR_HIERARCHY_DAY(load_dt::DATE, 3, 2);