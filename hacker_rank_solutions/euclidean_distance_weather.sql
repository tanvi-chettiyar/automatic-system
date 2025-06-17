-- Consider P1(a,b) and P2(c,d) to be two points on a 2D plane where a is equal to the minimum value in 
-- Northern Latitude, b is equal to the minimum value in Western Longitude, c is equal to maximum value 
-- in Northern Latitude and d is equal to maximum value in Western Longitude in Station. Query the Euclidean 
-- distance between P1 and P2 and round it to a scale of 4 decimal points.
select round(sqrt(power((max(lat_n)-min(lat_n)),2) + power((max(long_w)-min(long_w)),2)),4)
from station;