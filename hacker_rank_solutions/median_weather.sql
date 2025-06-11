with lat as (
select lat_n, row_number() over (order by lat_n) as rnk, count(*) over() as total
from station
)
select 
    round(avg(lat_n), 4)
from lat
where rnk in (floor((total+1)/2), ceiling((total+1)/2));