 -- Deduplicating the game details wit the row number operator
 --The game details will remove duplicated row on the query after the cte which adds the row number column
 with deduped AS(
select *, row_number() over (partition by player_id,game_id) as row_num
from game_details)
select * from deduped where row_num = 1