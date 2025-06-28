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

insert
	into
	public.actors 
with previous_year as (
	select
		*
	from
		public.actors
	where
		current_year = 1971
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
		"year" = 1972
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
