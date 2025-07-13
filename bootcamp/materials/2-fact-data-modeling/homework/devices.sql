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


drop table if exsits public.events_stg;

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
	user_id,
	browser_type,
	"date"
from
	base_data
where
	rn = 1

)

select * into public.events_stg from base_data_deduplicated;

with min_max_date as (
select
	min("date") as min_date,
	max("date") as max_date
from
	public.events_stg

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
	public.events_stg
where
	"date" = '2023-01-01'
		
),

yesterday as (
select
	*
from
	public.user_devices_cumulated
where
	datestamp = '2022-12-31'
)

select  
	coalesce(yesterday.user_id, today.user_id) as user_id
from
	yesterday as yesterday
full join today as today
	on
	yesterday.user_id = today.user_id;
