CREATE temp TABLE qwe1(client varchar, date date);
truncate qwe1;
insert into qwe1 values
(0,'2021-01-19'),
(0,'2021-01-28'),
(0,'2021-01-28'),
(0,'2021-02-02'),
(0,'2021-02-08'),
(0,'2021-02-15'),
(0,'2021-06-16'),
(0,'2021-06-28'),
(0,'2021-07-07'),
(0,'2021-07-12'),
(0,'2022-04-18'),
(0,'2022-07-23'),
(0,'2022-07-28'),
(1,'2022-08-22'),
(1,'2022-08-22'),
(1,'2022-08-23'),
(1,'2022-08-23'),
(1,'2022-08-23'),
(1,'2022-08-23'),
(2,'2022-05-22'),
(2,'2022-05-29'),
(2,'2022-02-23'),
(2,'2022-02-28'),
(2,'2022-08-22'),
(2,'2022-08-23'),
(3, '2022-09-22'),
(3, '2022-09-26'),
(3, '2022-10-03')

select * from qwe1

-- create calendar
with RECURSIVE dates(dt) as (
select min(date) from qwe1
union all 
select cast(d.dt + interval '1 days' as date) as dt from dates d
where dt < (select max(date) from qwe1)
), 

-- join calendar with main data table
join_table as (
select * 
from dates t1
left join qwe1 t2 on t1.dt = t2.date),

-- define date from and date to
recurs as 
  (select distinct
  	client,
    min(date) over(partition by client) as date_wind_from,
    cast(min(date) over(partition by client) + interval '7 days' as date) as date_wind_to
  from join_table
    union all
  select distinct
  	q2.client,
    min(q2.date) over(partition by q2.client) as date_wind_from,
    cast(min(date) over(partition by q2.client) + interval '7 days' as date) as date_wind_to
  from join_table as q2
    inner join recurs as recurs 
    on q2.date >= recurs.date_wind_to 
    and q2.client = recurs.client
    )
  
-- final query
select t1.client, t1.date, t2.date_wind_from, t2.date_wind_to,
row_number() over (partition by t1.client, date_wind_from order by t1.client, dt) as number_of_visits_in_wind, 
DENSE_RANK() over (partition by t1.client order by t1.client, date_wind_from) as wind_of_visits 
from join_table as t1
inner join recurs as t2 
on t1.dt between t2.date_wind_from and t2.date_wind_to 
and t1.client = t2.client
where date is not null
order by t1.client, t1.dt