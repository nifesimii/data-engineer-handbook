
DO $$
DECLARE
    year_start INT := 1970; -- Starting year
    year_end INT := 2021;   -- Ending year
    asofyear INT;       -- Loop variable for previous year
	asofthisyear INT;       -- Loop variable for this year
BEGIN
	-- Loop from the starting year to the ending year
	FOR asofyear IN year_start..year_end LOOP
	asofthisyear := asofyear + 1;
	insert into actors
	WITH last_year AS (
		SELECT * FROM actors
		WHERE current_year = asofyear
	),
	this_year AS (
	SELECT actor,
	actorid,
	YEAR,
	CASE WHEN year IS NULL THEN ARRAY[]::films[]
	ELSE ARRAY_AGG(ROW(film, votes, rating, filmid)::films)
	END AS films,
	avg(rating) as average_rating
	from actor_films af 
	where year = asofthisyear
	group by actor,actorid,year)
	SELECT 
	    COALESCE(ty.actor, ly.actor) actor,
	    COALESCE(ty.actorid , ly.actorid) actorid,
	    COALESCE(ly.films, ARRAY[]::films[]) || 
	                     CASE WHEN ty.year IS NOT NULL THEN ty.films
	                     ELSE ARRAY[]::films[]
	            END as films,
	 	case when array_length(ty.films,1) is not null then
		case WHEN ty.average_rating > 8 THEN 'star'
		     	WHEN ty.average_rating > 7 AND average_rating <= 8 THEN 'good'
		     	WHEN average_rating > 6 AND average_rating <= 7 THEN 'average'
		     	WHEN average_rating <= 6 THEN 'bad'
		     	end::quality_class 
		     	else ly.quality_class
		     	end as quality_class,           
	    COALESCE(ty.year,ly.current_year+1) as current_year,
	    case when array_length(ty.films,1) is null then false
	    else true 
	    end as is_active
	FROM last_year ly
	FULL OUTER JOIN this_year ty
	    on ly.actorid = ty.actorid;   
	END LOOP;
END $$;