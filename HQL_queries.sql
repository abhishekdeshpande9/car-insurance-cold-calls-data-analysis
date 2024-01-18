-- problem 1: data loading
create table car_insurance_data (
id int, 
age int, 
job string, 
marital string,
education string,
default int,
balance int,
hhinsurance int, 
carloan int,
communication string,
lastcontactday int,
lastcontactmonth string,
noofcontacts int,
dayspassed int,
prevattempts int,
outcome string,
callstart string,
callend string,
carinsurance int 
) 
row format delimited 
fields terminated by ','
tblproperties ('skip.header.line.count'='1')
stored as textfile;

-- load data inpath '/input_data/car_insurance_cold_calls_dataset.csv' into table car_insurance_data;

show tables;

select * from car_insurance_data limit 5;
-- set hive.cli.print.header = true;
select * from car_insurance_data limit 5;

-- problem 2: data exploration
-- 1. 
select count(id) as total_records from car_insurance_data; 

-- 2. 
select count(distinct job) as unique_job_categories from car_insurance_data; 

-- 3. 
select case
when age <18 then '18'
when (age >= 18 and age <= 30) then '18-30'
when (age >= 31 and age <= 45) then '31-45'
when (age >= 46 and age <= 60) then '46-60'
else '61+'
end as age_group,
count(*) as customer_count  
from car_insurance_data
group by case
when age <18 then '18'
when (age >= 18 and age <= 30) then '18-30'
when (age >= 31 and age <= 45) then '31-45'
when (age >= 46 and age <= 60) then '46-60'
else '61+'
end order by age_group;

-- 4.
select count(*) as records_with_missing_values
from car_insurance_data
where 
id is null or  
age is null or  
job is null or  
marital is null or  
education is null or  
default is null or  
balance is null or  
hhinsurance is null or  
carloan is null or  
communication is null or  
lastcontactday is null or  
lastcontactmonth is null or  
noofcontacts is null or  
dayspassed is null or  
prevattempts is null or  
outcome is null or  
callstart is null or  
callend is null or  
carinsurance is null;

-- 5.
select outcome, count(outcome) as unique_outcome_values
from car_insurance_data
group by outcome;

-- 6. 
select count(id) no_of_car_loan_and_home_insurance_customers
from car_insurance_data
where carloan = 1 and hhinsurance = 1;

-- problem 3: aggregations
-- 1. 
select job, avg(balance) as avg_balance, max(balance) as max_balance, min(balance) as min_balance
from car_insurance_data
group by job;

-- 2.
select carinsurance, count(*) as count
from car_insurance_data
group by carinsurance;

-- 3.
select communication, count(*) as count
from car_insurance_data
group by communication;

-- 4.
select communication, sum(balance) as balance_sum
from car_insurance_data
group by communication;

-- 5.
select outcome, sum(prevattempts) as no_of_prevattempts
from car_insurance_data
group by outcome;

-- 6.
select carinsurance, round(avg(noofcontacts), 2) as avg_no_of_contacts
from car_insurance_data
group by carinsurance;


-- problem 4: partitioning and bucketing
-- 1.
create table car_insurance_data_partitioned (
id int, 
age int, 
job string, 
default int,
balance int,
hhinsurance int, 
carloan int,
communication string,
lastcontactday int,
lastcontactmonth string,
noofcontacts int,
dayspassed int,
prevattempts int,
outcome string,
callstart string,
callend string,
carinsurance int)
partitioned by (education string, marital string)
row format delimited 
fields terminated by ',' 
stored as textfile;

-- set hive.exec.dynamic.partition.mode=nonstrict

insert overwrite table car_insurance_data_partitioned 
partition(education, marital) 
select id, age, job, default, balance, hhinsurance, carloan, communication, lastcontactday, lastcontactmonth, noofcontacts, dayspassed, prevattempts, outcome, callstart, callend, carinsurance, education, marital 
from car_insurance_data;

-- 2.
-- set hive.enforce.bucketing=true;

create table car_insurance_data_bucketed (
id int, 
age int, 
job string, 
marital string,
education string,
default int,
balance int,
hhinsurance int, 
carloan int,
communication string,
lastcontactday int,
lastcontactmonth string,
noofcontacts int,
dayspassed int,
prevattempts int,
outcome string,
callstart string,
callend string,
carinsurance int)
clustered by (age) into 4 buckets
row format delimited 
fields terminated by ',' 
stored as textfile;

-- insert overwrite table car_insurance_data_bucketed select * from  car_insurance_data;

-- 3.
create table car_insurance_data_partitioned_new (
id int, 
age int, 
default int,
balance int,
hhinsurance int, 
carloan int,
communication string,
lastcontactday int,
lastcontactmonth string,
noofcontacts int,
dayspassed int,
prevattempts int,
outcome string,
callstart string,
callend string,
carinsurance int)
partitioned by (education string, marital string, job string)
row format delimited 
fields terminated by ',' 
stored as textfile;

-- dynamic partition strict mode requires at least one static partition column. to turn this off 
-- set hive.exec.dynamic.partition.mode=nonstrict

-- set hive.exec.max.dynamic.partitions.pernode=200;

/* 
insert overwrite table car_insurance_data_partitioned_new
partition(education, marital, job) 
select id, age, default, balance, hhinsurance, carloan, communication, lastcontactday, lastcontactmonth, noofcontacts, dayspassed, prevattempts, outcome, callstart, callend, carinsurance, education, marital, job
from car_insurance_data_partitioned; 
*/

-- 4.
-- set hive.enforce.bucketing=true;

create table car_insurance_data_bucketed_new (
id int, 
age int, 
job string, 
marital string,
education string,
default int,
balance int,
hhinsurance int, 
carloan int,
communication string,
lastcontactday int,
lastcontactmonth string,
noofcontacts int,
dayspassed int,
prevattempts int,
outcome string,
callstart string,
callend string,
carinsurance int)
clustered by (age) into 10 buckets
row format delimited 
fields terminated by ',' 
stored as textfile;

-- insert overwrite table car_insurance_data_bucketed_new select * from car_insurance_data_bucketed;



-- problem 5: optimized joins
-- 1.
select o.job as job, p.education as education_level, avg(o.balance) as avg_balance
from car_insurance_data o
join car_insurance_data_partitioned_new p 
on o.id = p.id
group by o.job, p.education;

-- 2.
select b.age as age_group, sum(o.noofcontacts) as total_no_of_contacts 
from car_insurance_data o
join car_insurance_data_bucketed_new b
on o.id = b.id
group by b.age;

-- 3.
select p.age as age, p.education as education_level, p.marital as marital_status, sum(b.balance) as total_balance
from car_insurance_data_partitioned_new p 
join car_insurance_data_bucketed_new b
on p.id = b.id
group by p.age, p.education, p.marital;

-- problem 6: window function
-- 1.
select age, job, noofcontacts, 
sum(noofcontacts) over (partition by job order by age) as cumulative_sum
from car_insurance_data
order by age;

-- 2.
select age, job, balance, 
round(avg(balance) over (partition by job order by age), 2) as running_avg
from car_insurance_data
order by age;

-- 3.
select job, age, balance
from (select age, job, balance, 
row_number() over(partition by job, age order by balance desc) as rank
from car_insurance_data) x
where x.rank = 1
order by job, age;

-- 4.
select job, balance, 
rank() over(partition by job order by balance desc) as balance_rank
from car_insurance_data
order by job;

-- problem 7: advanced aggregations
-- 1.
select job, total_car_insurances
from (
select job, sum(carinsurance) as total_car_insurances
from car_insurance_data
group by job ) x
order by total_car_insurances desc
limit 1;

-- 2.
select lastcontactmonth as last_contact_month, count(*) as total_no_of_contacts
from car_insurance_data
group by lastcontactmonth
order by total_no_of_contacts desc
limit 1;

-- 3.
select ci1.job, round(ci1.car_insurance_count / ci0.no_car_insurance_count, 2) as car_insurance_ratio
from (
select job, count(*) as no_car_insurance_count
from car_insurance_data
where carinsurance = 0
group by job) ci0
join (
select job, count(*) as car_insurance_count
from car_insurance_data
where carinsurance = 1
group by job) ci1
on ci1.job = ci0.job;

-- 4.
select job, education, sum(carinsurance) as no_of_carinusrance
from car_insurance_data
group by job, education
order by no_of_carinusrance desc
limit 1;

-- 5.
select job, outcome, avg(noofcontacts) as avg_no_of_contacts
from car_insurance_data
group by job, outcome
order by avg_no_of_contacts;

-- 6.
select lastcontactmonth as month, sum(balance) as total_balance
from car_insurance_data
group by lastcontactmonth
order by total_balance desc
limit 1;


-- problem 8: complex joins and aggregations
-- 1.
select education as education_level, round(avg(balance), 2) as avg_balance
from car_insurance_data
where (hhinsurance = 1 and carloan = 1)
group by education
order by avg_balance;

-- 2.
select communication as communication_type, round(avg(noofcontacts), 2) as avg_no_of_contacts
from car_insurance_data
where carinsurance = 1
group by communication
order by avg_no_of_contacts desc
limit 3;

-- 3.
select job, round(avg(balance), 2) as avg_balance
from car_insurance_data
where carloan = 1
group by job
order by avg_balance;

-- 4.
select job, round(avg(balance), 2) as avg_balance
from (
select job, balance
from car_insurance_data
where default = 1) x
group by job
order by count(*) desc
limit 5;

-- problem 9: advanced window functions
-- 1.
With ranked_customers as (
Select job, id, noofcontacts,
rank() over(partition by job order by noofcontacts) as rank
From car_insurance_data)

Select rc1.job as job, rc1.id as id, rc1.noofcontacts as noofcontacts,
rc2.noofcontacts as next_highest_no_of_contacts,
coalesce(rc2.noofcontacts, rc1.noofcontacts) - rc1.NoOfContacts as difference
from ranked_customers rc1
left join ranked_customers rc2
on rc1.job - rc2.job and rc1.rank = rc2.rank + 1;

-- 2.
select c.id, c.job, c.balance, c.balance - avgb.avg_balance as balance_difference
from car_insurance_data c
join ( 
select job, round(avg(balance), 2) as avg_balance
from car_insurance_data
group by job ) avgb
on c.job = avgb.job;

-- 3.
select job, id, callduration as longest_call_duration
from (
select job, id, callduration,
row_number() over(partition by job order by callduration desc) as rank
from (
select from_unixtime(unix_timestamp(callend, 'hh:mm:ss') - unix_timestamp(callstart, 'hh:mm:ss'), 'hh:mm:ss') as callduration, job, id
from car_insurance_data) cd 
) rn
where rank = 1
order by job;

-- 4.
select id, job, noofcontacts,
avg(noofcontacts) over(partition by job order by id
rows between 2 preceding and current row) as moving_average
from car_insurance_data;

