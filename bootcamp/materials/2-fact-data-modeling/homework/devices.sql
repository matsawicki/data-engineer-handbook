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


)
select
	*
from
	date_ranges