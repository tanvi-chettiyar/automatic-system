-- Write a query to output the names of those students whose best friends got offered a higher salary than them. 
-- Names must be ordered by the salary amount offered to the best friends. It is guaranteed that no two students 
-- got same salary offer.
select st.name
from friends f
left join packages p on (f.id = p.id)
left join packages p2 on (f.friend_id = p2.id)
left join students st on (f.id = st.id)
where p2.salary > p.salary
order by p2.salary asc;