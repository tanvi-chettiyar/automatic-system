-- Write a query to print the hacker_id, name, and total score of the hackers ordered by the descending score. 
-- If more than one hacker achieved the same total score, then sort the result by ascending hacker_id. Exclude 
-- all hackers with a total score of from your result.
select s.hacker_id, h.name, sum(s.max_score) as total
from (    
    select hacker_id, challenge_id, max(score) as max_score
    from submissions s
    group by hacker_id, challenge_id
) s
left join hackers h on (h.hacker_id = s.hacker_id)
group by s.hacker_id, h.name
having total > 0
order by total desc, hacker_id asc;