-- Pivot the Occupation column in OCCUPATIONS so that each Name is sorted alphabetically and displayed underneath its 
-- corresponding Occupation. The output should consist of four columns (Doctor, Professor, Singer, and Actor) in that 
-- specific order, with their respective names listed alphabetically under each column.
select bbb.doctors, bbb.professors, bbb.singers, bbb.actors
from (
select aaa.rnk,
    max(case when aaa.occupation='Doctor' then aaa.name end) as doctors,
    max(case when aaa.occupation='Professor' then aaa.name end) as professors,
    max(case when aaa.occupation='Singer' then aaa.name end) as singers,
    max(case when aaa.occupation='Actor' then aaa.name end) as actors
from (
select name, occupation, rank() over (partition by occupation order by name) as rnk
from occupations
) as aaa
group by rnk) as bbb
;