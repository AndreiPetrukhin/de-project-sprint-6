WITH first_groups AS (
    SELECT
        g.hk_group_id
    FROM
        stv202404106__DWH.h_groups g
    ORDER BY
        g.registration_dt
    LIMIT 10
),

added_users AS (
    SELECT
        g.hk_l_user_group_activity
    FROM
        stv202404106__DWH.s_auth_history g
    WHERE
        g.event = 'add'
    GROUP BY
        g.hk_l_user_group_activity
    ORDER BY
        g.hk_l_user_group_activity
),

user_group_log AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users
    FROM
        stv202404106__DWH.l_user_group_activity luga
    JOIN
        first_groups fg ON fg.hk_group_id = luga.hk_group_id
    LEFT JOIN
        added_users au ON au.hk_l_user_group_activity = luga.hk_l_user_group_activity
    WHERE
        au.hk_l_user_group_activity IS NULL
    GROUP BY
        luga.hk_group_id
)

SELECT
    hk_group_id,
    cnt_added_users
FROM
    user_group_log
ORDER BY
    cnt_added_users DESC
LIMIT 10;