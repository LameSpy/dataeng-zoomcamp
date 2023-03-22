/*
create temp table gas_station (month int, gas int)
truncate gas_station;

insert into gas_station values
(1, 30),
(2, 50),
(3, 50),
(4, 50),
(5, 50),
(6, 50),
(7, 50),
(8, 30),
(9, 50),
(10, 50),
(11, 50),
(12, 50)*/

with recursive flag as (
select *,
--case when gas >= 50 then 1 else 0 end t,
case when 
	sum(case when gas >= 50 then 1 else 0 end) 
	over (order by month rows between 2 preceding and current row) 
	= 3 then 1 else 0 end as  flag
from gas_station
),

bonus as (
select t.*, 0 as n1, 0 as n2, 0 as n3 
from flag t
where t.month = 1

-- recursion
union all 
select t2.month, t2.gas, t2.flag, 

t.n2 as t1,
t.n3 as t2,
case when t.n2 = 0 and t.n3 = 0 and t2.flag = 1 then 1 else 0 end n1

from bonus t
join flag t2 on t.month = t2.month - 1
)
select *
from bonus


