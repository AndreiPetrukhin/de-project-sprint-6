WITH first_groups AS (
    SELECT
        hk_group_id,
        registration_dt
    FROM
        stv202404106__DWH.h_groups
    ORDER BY
        registration_dt
    LIMIT 10
),

user_with_messages AS (
    SELECT
        hk_user_id
    FROM
        stv202404106__DWH.l_user_message
    GROUP BY
        hk_user_id
),

added_users AS (
    SELECT
        luga.hk_group_id,
        COUNT(DISTINCT CASE WHEN sah.event = 'add' AND sah.hk_l_user_group_activity IS NOT NULL THEN luga.hk_user_id END) AS cnt_added_users,
        COUNT(DISTINCT CASE WHEN uwm.hk_user_id IS NOT NULL THEN luga.hk_user_id END) AS cnt_users_in_group_with_messages
    FROM
        stv202404106__DWH.l_user_group_activity luga
    JOIN 
        first_groups fg ON fg.hk_group_id = luga.hk_group_id
    LEFT JOIN
        stv202404106__DWH.s_auth_history sah ON sah.hk_l_user_group_activity = luga.hk_l_user_group_activity
    LEFT JOIN
        user_with_messages uwm ON uwm.hk_user_id = luga.hk_user_id
    GROUP BY
        luga.hk_group_id
    --having cnt_added_users < cnt_users_in_group_with_messages -- to check results
)

SELECT
    hk_group_id,
    COALESCE(cnt_added_users, 0) AS cnt_added_users,
    COALESCE(cnt_users_in_group_with_messages, 0) AS cnt_users_in_group_with_messages,
    CASE
        WHEN COALESCE(cnt_added_users, 0) > 0 THEN 
            ROUND(COALESCE(cnt_users_in_group_with_messages, 0)::FLOAT / cnt_added_users, 2)
        ELSE 
            0 
    END AS group_conversion
FROM
    added_users;