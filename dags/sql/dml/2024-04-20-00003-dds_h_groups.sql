-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00003-dds_h_groups.sql
INSERT INTO stv202404106__DWH.h_groups(hk_group_id, group_id, registration_dt, load_dt, load_src)
SELECT
    hash(id) AS hk_group_id,
    id AS group_id,
    registration_dt,
    now() AS load_dt,
    's3' AS load_src
FROM STV202404106__STAGING.groups
WHERE hash(id) NOT IN (SELECT hk_group_id FROM stv202404106__DWH.h_groups);