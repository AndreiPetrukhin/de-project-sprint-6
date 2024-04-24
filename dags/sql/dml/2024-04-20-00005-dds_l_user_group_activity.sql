-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00005-dds_l_user_group_activity.sql
-- Вставка данных в таблицу l_user_group_activity
INSERT INTO stv202404106__DWH.l_user_group_activity (hk_l_user_group_activity, hk_user_id, hk_group_id, load_dt, load_src)

SELECT DISTINCT
    gl.event_id AS hk_l_user_group_activity,
    hu.hk_user_id AS hk_user_id,
    hg.hk_group_id AS hk_group_id,
    NOW() AS load_dt,
    's3' AS load_src

FROM stv202404106__STAGING.group_log AS gl
LEFT JOIN stv202404106__DWH.h_users AS hu ON gl.user_id = hu.user_id
LEFT JOIN stv202404106__DWH.h_groups AS hg ON gl.group_id = hg.group_id;