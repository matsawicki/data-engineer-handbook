
/*
create type browser_date_list as (
	browser_type text,
	date_list date[]
);

create table user_devices_cumulated (
	user_id numeric,
	datestamp date,
	browser_date_list browser_date_list,
	primary key (user_id, datestamp)
);
*/
with base_data as (
select
	events.user_id,
	devices.browser_type,
	events.event_time::date as "date",
	row_number() over(partition by events.user_id,
	devices.browser_type,
	events.event_time::date ) as rn
from
	public.events as events
inner join public.devices as devices
	on
	events.device_id = devices.device_id
where
	events.user_id is not null  
	
),

base_data_deduplicated as (
select
	*
from
	base_data
where
	rn = 1

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
select
	*
from
	public.user_devices_cumulated
where datestamp = '2022-12-31'
),

today as (
select 
		user_id,
		"date" as datestamp,
		row(browser_type,
		array[
			"date"
		]		
		)::browser_date_list as browser_date_list
from
	base_data_deduplicated
where
	"date" = '2023-01-01'
		
)

select  
	coalesce(yesterday.user_id, today.user_id) as user_id
	
from
	yesterday as yesterday
full join today as today
	on
	yesterday.user_id = today.user_id;
