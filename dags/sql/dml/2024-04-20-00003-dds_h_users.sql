-- /Users/apetrukh/Desktop/de_yandex/de-project-sprint-6/dags/sql/dml/2024-04-20-00003-dds_h_users.sql
INSERT INTO stv202404106__DWH.h_users(hk_user_id, user_id,registration_dt,load_dt,load_src)
select
       hash(id) as  hk_user_id,
       id as user_id,
       registration_dt,
       now() as load_dt,
       's3' as load_src
       from STV202404106__STAGING.users
where hash(id) not in (select hk_user_id from stv202404106__DWH.h_users);