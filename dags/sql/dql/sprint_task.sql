SELECT
  s.age,
  COUNT(DISTINCT l.hk_user_id) AS user_count
FROM
  stv202404106__DWH.s_user_socdem s
JOIN
  stv202404106__DWH.l_user_message l ON s.hk_user_id = l.hk_user_id
WHERE
  l.hk_message_id IN (
    SELECT
      lgd.hk_message_id
    FROM
      stv202404106__DWH.l_groups_dialogs lgd
    WHERE
      lgd.hk_group_id IN (
        SELECT
          g.hk_group_id
        FROM
          stv202404106__DWH.h_groups g
        ORDER BY
          g.registration_dt
        LIMIT 10
      )
  )
GROUP BY
  s.age
ORDER BY
  user_count DESC;