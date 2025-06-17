-- Julia asked her students to create some coding challenges. Write a query to print the hacker_id, name, and 
-- the total number of challenges created by each student. Sort your results by the total number of challenges 
-- in descending order. If more than one student created the same number of challenges, then sort the result by 
-- hacker_id. If more than one student created the same number of challenges and the count is less than the maximum 
-- number of challenges created, then exclude those students from the result.
select h.hacker_id, h.name, count(c.challenge_id) as total
from hackers h
left join challenges c on (c.hacker_id = h.hacker_id)
group by h.hacker_id, h.name
having 
    count(c.challenge_id) = (
        select max(cnt)
        from (
            select count(*) as cnt
            from challenges
            group by hacker_id
            ) as maxch
        ) 
    or count(c.challenge_id) in (
        select total 
        from (
            select hacker_id, count(*) as total
            from challenges
            group by hacker_id
            ) as subcnt
        group by total
        having count(*) = 1
        )
order by total desc, h.hacker_id asc;