with user_with_messages as (
    select
    	hk_user_id
    from stv202404106__DWH.l_user_message
    group by hk_user_id
    order by hk_user_id asc
),

user_group_messages as (
	select
		luga.hk_group_id
		, count(distinct luga.hk_user_id) as cnt_users_in_group_with_messages
	FROM stv202404106__DWH.l_user_group_activity luga 
	join user_with_messages uwm on uwm.hk_user_id = luga.hk_user_id
	GROUP by hk_group_id
)
select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10

;