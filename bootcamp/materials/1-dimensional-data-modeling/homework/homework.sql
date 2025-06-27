--1 DDL for actors table

drop type if exists films cascade;

create type films as (
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
	actorid text primary key,
	actor text,
	current_year int4,
	films films,
	quality_class quality_class,
	is_active bool

);

with previous_year as (
select
	*
from
	public.actors
where
	current_year = 1969
),

current_year as (
select
	*
from
	public.actor_films
where
	"year" = 1970
)


select
	coalesce(ct.actorid, py.actorid) as actorid,
	coalesce(ct.actor, py.actor) as actor,
	coalesce(ct.year, py.current_year) -- ogarnac dobrze logike dla roku
	case
		when py.films is null then 
			array[
				row(
					ct.filmid,
					ct.film,
					ct.votes,
					ct.rating
					)::films
				  ]
		else py.films || array[
							row(
								ct.filmid,
								ct.film,
								ct.votes,
								ct.rating
								)::films
							  ]
	end as films
from
		current_year as ct
full join previous_year as py
	on
		ct.actorid = py.actorid;
