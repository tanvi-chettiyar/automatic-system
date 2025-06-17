-- Given the table schemas below, write a query to print the company_code, founder name, total number of lead
-- managers, total number of senior managers, total number of managers, and total number of employees. Order 
-- your output by ascending company_code.
select 
    CT.company_code, 
    CT.founder, 
    count(Distinct LMT.lead_manager_code) as lead_manager_count, 
    count(Distinct SMT.senior_manager_code) as senior_manager_count, 
    count(Distinct MT.manager_code) as manager_count, 
    count(Distinct ET.employee_code) as employee_count
from Company CT
left join Lead_Manager LMT on (CT.company_code=LMT.company_code)
left join Senior_Manager SMT on (CT.company_code=SMT.company_code)
left join Manager MT on (CT.company_code=MT.company_code)
left join Employee ET on (CT.company_code=ET.company_code)
group by CT.company_code, CT.founder
order by CT.company_code ASC;