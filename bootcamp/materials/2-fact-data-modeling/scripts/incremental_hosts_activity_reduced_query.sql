--This is the query for an incremental query that loads host_activity_reduced on a daily basis
insert into hosts_activity_reduced
with daily_aggregate as (
    select
    	date(event_time) as date, -- date extracted from the events time timestamp
        host, -- host column
        count(1) as num_site_hits, -- number of sites hits
        count(distinct user_id) as num_visitor_hits -- number of sites hits by unique visitors
    where date(event_time) = date('2023-01-31')
    and host is not null
    group by host, date(event_time)
),
    yesterday_array as (
        select *
        from hosts_activity_reduced
        where month = date('2023-01-01') -- hardcoded
    )
select
	coalesce(ya.month, date_trunc('month', da.date)) as month,
    coalesce(da.host, ya.host) as host,
   'hit_num' as  hit_metric,
    case
        when ya.hit_array is not null then ya.hit_array|| array[coalesce(da.num_site_hits, 0)]::real[]
        when ya.hit_array is null then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]::int[]
    end as hit_array,
   'unique_visitors_num' as  unique_visitors_metric,
    case
        when ya.unique_visitors_array is not null then ya.unique_visitors_array|| array[coalesce(da.num_visitor_hits, 0)]::real[]
        when ya.unique_visitors_array is null then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.num_visitor_hits, 0)]::int[]
    end as unique_visitors_array   
from daily_aggregate da full outer join yesterday_array ya
ON da.host = ya.host
ON conflict (month,host,hit_metric,unique_visitors_metric)
DO
    update set hit_array = excluded.hit_array,unique_visitors_array = excluded.unique_visitors_array ;  -- the merge column based on hit_array and unique_visitors_array so to overwrite duplicates column 