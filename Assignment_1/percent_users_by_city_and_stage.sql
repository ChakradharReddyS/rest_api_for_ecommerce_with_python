
--Percentage of users in each stage of user journey

with user_count_by_stage
as
(
select distinct event_type
, count(user_id) over (partition by event_type) as users_count
, count(user_id) over () as overall_user_event_count
from app_events_table_v3
)

select event_type,(users_count*100/overall_user_event_count) as percent_users_by_stage 
from user_count_by_stage

--Percentage of users in each city

with user_count_by_city
as
(
select distinct city
, count(user_id) over (partition by city) as users_count
, count(user_id) over () as overall_user_event_count
from app_events_table_v3
)

select city,(users_count*100/overall_user_event_count) as percent_users_by_city 
from user_count_by_city



