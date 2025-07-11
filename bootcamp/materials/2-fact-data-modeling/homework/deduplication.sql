
with deduplicated_game_details as (
select
	*,
	row_number() over(partition by game_id, player_id) as rn -- this is a real primary key - each player can perform only in one team per game.
from
	public.game_details 

)
select
	*
from
	deduplicated_game_details
where
	rn = 1