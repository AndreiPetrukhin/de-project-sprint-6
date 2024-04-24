INSERT INTO stv202404106__DWH.s_user_socdem (hk_user_id, country, age, load_dt, load_src)
SELECT
  hu.hk_user_id,
  u.country,
  u.age,
  NOW() AS load_dt,
  's3' AS load_src
FROM stv202404106__STAGING.users AS u
LEFT JOIN stv202404106__DWH.h_users AS hu ON u.id = hu.user_id;