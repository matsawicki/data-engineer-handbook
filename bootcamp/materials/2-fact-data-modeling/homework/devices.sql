create type browser_date_list as (
	browser_type text,
	date_list date[]
);


create table user_devices_cumulated (
	user_id numeric primary key,
	browser_date_list browser_date_list
);

with base_data as (
select
	events.user_id,
	devices.browser_type,
	events.event_time::date as "date"
from
	public.events as events
inner join public.devices as devices
	on
	events.device_id = devices.device_id
where
	events.user_id is not null 
),
min_max_date as (
select
	min("date") as min_date,
	max("date") as max_date
from
	base_data

),
date_ranges as (
select
	series.series::date as all_dates
from
	min_max_date as dates
inner join lateral generate_series(dates.min_date, dates.max_date, interval '1 day') as series
on
	true


),

yesterday as (
	select * from base_data 
	where "date" = '2022-12-31'
),

today as (
	select * from user_devices_cumulated
)


select  
	coalesce(yesterday.user_id, today.user_id) as user_id

from today as today
full join yesterday as yesterday
	on yesterday.user_id = today.user_id
	and yesterday.browser_type = today.browser_date_list.browser_type
