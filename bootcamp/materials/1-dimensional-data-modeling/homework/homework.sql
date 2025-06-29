--1 DDL for actors table

drop type if exists films cascade;

create type films as (
	"year" int4,
	filmid text,
	film text,
	votes int4,
	rating float4
);

drop type if exists quality_class cascade;

create type quality_class as enum ('star',
'good',
'average',
'bad');

drop table if exists actors;

create table actors (
	actorid text ,
	actor text,
	current_year int4,
	films films[],
	quality_class quality_class,
	is_active bool,
	primary key (actorid,
current_year)

);
-- 2 Cumulative table generation query

select
	min("year") as first_year,
	max("year") as last_year
from
	actor_films 

do $$
declare
var_current_year integer := 1969;

begin
while var_current_year < 2021 loop
raise notice 'processing year  %',
var_current_year;

insert
	into
	public.actors 
with previous_year as (
	select
		*
	from
		public.actors
	where
		current_year = var_current_year
),


	current_year as (
	select
		actorid,
		actor,
		"year",
		array_agg(row("year", filmid, film, votes, rating)::films) as films,
		avg(rating) as average_rating
	from
		public.actor_films
	where
		"year" = var_current_year + 1
	group by
		actorid,
		actor,
		"year"
		),
	current_year_with_quality_class as (
	select
		* ,
		case
			when average_rating <= 6 then 'bad'
			when average_rating <= 7 then 'average'
			when average_rating <= 8 then 'good'
			else 'star'
		end as quality_class
	from
		current_year

)


select
	coalesce(ct.actorid, py.actorid) as actorid,
	coalesce(ct.actor, py.actor) as actor,
	coalesce(ct.year, py.current_year + 1) as current_year, 
	case
		when py.films is null then 
			ct.films
		else py.films || ct.films
	end as films,
	ct.quality_class::quality_class as quality_class,
	case
		when ct.films is not null then true
		else false
	end as is_active
from
		current_year_with_quality_class as ct
full join previous_year as py
	on
		ct.actorid = py.actorid;

var_current_year := var_current_year + 1;
end loop;

end$$;
-- 3 and 4 DDL for actors_history_scd table 


create table actors_history_scd (
	actorid text,
	actor text,
	is_active bool,
	quality_class quality_class,
	start_date integer,
	end_date integer,
	current_year integer,
	primary key (actorid,
start_date,
end_date)

);

insert
	into
	actors_history_scd
with with_previous_value as (
	select
		actorid,
		actor,
		is_active,
		current_year,
		quality_class,
		lag(is_active) over(partition by actorid order by current_year) as previous_is_active,
		lag(quality_class) over(partition by actorid order by current_year) as previous_quality_class
	from
		actors 
),
	with_change_indicator as (
	select
		*,
		case
			when is_active <> previous_is_active then 1
			when quality_class <> previous_quality_class then 1
			else 0
		end as change_indicator
	from
		with_previous_value
),
	with_streaks as (
	select
		* ,
		sum(change_indicator) over(partition by actorid order by current_year) as streak_indentifier
	from
		with_change_indicator
),
	scd as (
	select
		actorid,
		actor,
		streak_indentifier,
		is_active,
		"quality_class",
		min(current_year) as start_date,
		max(current_year) as end_date
	from
		with_streaks
	group by
		actorid,
		actor,
		streak_indentifier,
		is_active,
		"quality_class" 
)

select  
	actorid,
	actor,
	is_active,
	quality_class,
	start_date,
	end_date,
	2021 as current_year
from
	scd
	
-- 5 Incremental query for actors_history_scd