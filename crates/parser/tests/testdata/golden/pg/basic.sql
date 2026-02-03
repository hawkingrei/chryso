select id, sum(amount) from sales where region = 'us' group by id order by id limit 2 offset 1
