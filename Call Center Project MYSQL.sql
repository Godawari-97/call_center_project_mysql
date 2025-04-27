-- Create the database
create database call_center;

--  Use the created database
use call_center;

 CREATE TABLE cust_spt_call (
    id VARCHAR(50) PRIMARY KEY,
    customer_name VARCHAR(100),
    sentiment VARCHAR(20),
    csat_score varchar(50),
    call_timestamp varchar(20),
    reason VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    channel VARCHAR(20),
    response_time VARCHAR(20),
    call_duration_in_minutes INT,
    call_center VARCHAR(50)
);

describe table  cust_spt_call;
select * from  cust_spt_call;


 

select * from  customer_calls;

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\call_center_data.csv" 
INTO TABLE call_center.cust_spt_call 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

select count(*) as Total_Records from mytable;

select * from mytable;

# lets see data 
SELECT * FROM mytable LIMIT 10;

describe mytable;

SET SQL_SAFE_UPDATES = 0;

UPDATE mytable SET call_timestamp = str_to_date(call_timestamp, "%m/%d/%Y");

UPDATE mytable SET csat_score = NULL WHERE csat_score = 0;

SET SQL_SAFE_UPDATES = 1;

SELECT * FROM mytable LIMIT 10;

describe mytable;

select call_timestamp,date(call_timestamp) from mytable;

-- ----------------------------------------------------------------------------- Exploring our data ------------------------------------------------------------------------------------------------------------------


# -- Checking the distinct values of some columns:

 select distinct  sentiment FROM  mytable;
select distinct reason FROM mytable;
select distinct channel FROM mytable;
select distinct response_time FROM mytable;
select distinct call_center FROM mytable;

select * from mytable;
-- The count and precentage from total of each of the distinct values we got:
select sentiment, count(*) as count, round((count(*) / (select count(*)
 from mytable)) *100,1) as pct  from mytable group by sentiment
order by pct desc;

select * from mytable;
  -- calculate for each reason in the mytable table, and how is the percentage value derived?
  select  reason, count(*), round((count(*) / (select count(*) from mytable)) * 100, 0) as pct
  from mytable group by reason order by pct desc;
  

-- calculate for each channel in the mytable table, and how is the percentage value rounded and ordered?
select channel, count(*) as count, round((count(*) / (select count(*) from mytable)) * 100, 1) as percentage
from mytable  group by channel order  by percentage desc;


-- calculate for each response_time in the mytable table, and how is the percentage value computed and sorted?
select response_time, count(*) as count, round((count(*) / (select count(*) from mytable)) * 100, 1) as percentage
from mytable group by response_time order by  percentage desc;



-- calculate for each call_center in the mytable table, and how is the percentage value computed and displayed in descending order?
select call_center, count(*) as count, round((count(*) / (select count(*) from mytable)) * 100, 1) as percentage
from mytable group by call_center order by percentage desc;



select * from mytable;
SELECT state, COUNT(*) FROM mytable GROUP BY 1 ORDER BY 2 DESC;

-- calculate for each day of the week based on the call_timestamp in the mytable table, and how is the result ordered?
select dayname(call_timestamp) as day_of_call, count(*) num_of_calls
 from mytable group by 1 order by 2 desc;


-- ------------------------------------------------------------------------ Aggregations----------------------------------------------------------------------------------------------------------------

-- calculate for the csat_score in the mytable table, excluding values equal to 0, and how is the average score rounded?
select min(csat_score) as min_score, max(csat_score) as max_recent, round(avg(csat_score), 1) as avg_score 
  from mytable  where csat_score !=0; 
  select * from mytable;
-- calculate for the call_timestamp in the mytable table, and what are the resulting columns labeled as?
select min(call_timestamp) as earliest_date, max(call_timestamp) as max_recent from mytable;

-- calculate for the call duration in minutes in the mytable table, and how are the results labeled?
select min(call_duration_in_minutes) as min_call_duration, max(call_duration_in_minutes) as max_call_duration, 
avg(call_duration_in_minutes) as avg_call_duration
from mytable;  


-- calculate for each combination of call_center and response_time in the mytable table, and how is the result ordered?
select call_center, response_time, count(*) as count
from mytable group by 1,2 order by 1,3 desc;


 -- calculate for each call_center in the mytable table, and how is the result ordered based on average call duration?
 select call_center, avg(call_duration_in_minutes) as avg_call_duration
 from mytable group by call_center order by avg_call_duration desc;
 

-- calculate for each channel in the mytable table, and how is the result ordered by average call duration?
select channel, round(avg(call_duration_in_minutes),2) as avg_call_duration 
from mytable  group by channel order by  avg_call_duration desc;

  select * from mytable;
-- calculate for each state in the mytable table, and how is the result ordered?
select state, count(*)  from mytable 
group by 1 order by 2 desc;


-- calculate for each combination of state and reason in the mytable table, and how is the result ordered?
select state, reason, count(*) from mytable
 group by 1,2 order by 1,2,3 desc;


-- calculate for each combination of state and sentiment in the mytable table, and how is the result ordered?
select state, sentiment, count(*) from mytable group by 1,2 order by 1,3 desc;


-- calculate for the average csat_score of each state in the mytable table, excluding scores equal to 0, and how is the result ordered?
select state, round(avg(csat_score),2) as avg_csat_score from mytable 
 where csat_score != 0 group by 1 order by 2 desc;


 -- calculate for each sentiment in the mytable table, and how is the result ordered based on average call duration?
 select sentiment,  round(avg(call_duration_in_minutes),2) as avg_call_duration 
 from mytable group by sentiment order by avg_call_duration desc;
 

-- calculate for each call_timestamp in the mytable table, and how is the result ordered based on the maximum call duration?
select call_timestamp,  max(call_duration_in_minutes)  over(partition by call_timestamp) as max_call_duration from mytable
 order by  max_call_duration  desc;




-- ----------------------------------------------------------------------------- Insightful Queries ------------------------------------------------------------------------------------------------------------------
-- Distribution of Calls by City:
SELECT city, COUNT(*) AS count, ROUND((COUNT(*) / (SELECT COUNT(*) FROM mytable)) * 100, 1) AS pct
FROM mytable GROUP BY city ORDER BY count DESC;


-- Average CSAT Score by Call Center:
SELECT call_center, AVG(csat_score) AS avg_csat_score
FROM mytable WHERE csat_score IS NOT NULL GROUP BY call_center ORDER BY avg_csat_score DESC;


-- Total and Average Call Duration by State:
SELECT state,SUM(`call duration in minutes`) AS total_call_duration, AVG(`call duration in minutes`) AS avg_call_duration
FROM mytable GROUP BY state ORDER BY total_call_duration DESC;


-- Sentiment Breakdown by Channel:
SELECT channel, sentiment, COUNT(*) AS count
FROM mytable GROUP BY channel, sentiment ORDER BY channel, count DESC;


-- Response Time Analysis by Call Center:
SELECT call_center, response_time, COUNT(*) AS count
FROM mytable GROUP BY call_center, response_time ORDER BY call_center, response_time;


-- Average Call Duration by Reason:
SELECT reason, AVG(`call duration in minutes`) AS avg_call_duration
FROM mytable GROUP BY reason ORDER BY avg_call_duration DESC;

select * from mytable;

-- Monthly Call Volume:
SELECT DATE_FORMAT(call_timestamp, '%Y-%m') AS month, COUNT(*) AS call_volume
FROM mytable GROUP BY month ORDER BY month DESC;

-- Call Volume by Day of the Week:
SELECT DAYNAME(call_timestamp) AS day_of_week, COUNT(*) AS call_volume
FROM mytable GROUP BY day_of_week 
ORDER BY FIELD(day_of_week, 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');


-- Call Volume by Hour of the Day:
SELECT HOUR(call_timestamp) AS hour_of_day, COUNT(*) AS call_volume
FROM mytable GROUP BY hour_of_day ORDER BY hour_of_day;


-- Correlation Between Call Duration and CSAT Score:
SELECT `call duration in minutes` AS call_duration, AVG(csat_score) AS avg_csat_score
FROM mytable WHERE csat_score IS NOT NULL GROUP BY `call duration in minutes` ORDER BY call_duration;


-- Analysis of Billing Question Calls:
SELECT state, COUNT(*) AS billing_call_count, AVG(`call duration in minutes`) AS avg_call_duration
FROM mytable WHERE reason = 'Billing Question' GROUP BY state ORDER BY billing_call_count DESC;


-- Call Center Performance Comparison:

SELECT call_center, SUM(CASE WHEN sentiment = 'Very Positive' THEN 1 ELSE 0 END) AS very_positive_count, SUM(CASE WHEN sentiment = 'Very Negative' THEN 1 ELSE 0 END) AS very_negative_count,
AVG(`call duration in minutes`) AS avg_call_duration,AVG(csat_score) AS avg_csat_score
FROM mytable GROUP BY call_center ORDER BY very_positive_count DESC, very_negative_count ASC;


-- Service Outage Call Analysis:
SELECT state, COUNT(*) AS service_outage_calls, AVG(`call duration in minutes`) AS avg_call_duration
FROM mytable WHERE reason = 'Service Outage' GROUP BY state ORDER BY service_outage_calls DESC;


-- Sentiment Analysis by State:
SELECT state, sentiment, COUNT(*) AS count
FROM mytable GROUP BY state, sentiment ORDER BY state, count DESC;

select * from mytable;
-- Response Time Impact on CSAT Score:
SELECT response_time, AVG(csat_score) AS avg_csat_score
FROM mytable WHERE csat_score IS NOT NULL GROUP BY response_time ORDER BY avg_csat_score DESC;


--  Find the total number of calls handled by each agent
select customer_name, count(*) as total_call_handle from mytable 
group by customer_name order by total_call_handle desc;

select customer_name, sentiment from mytable;

-- Find records where the sentiment is "Neutral"
select * from mytable where sentiment = "Neutral";
select * from mytable;
-- Get details of customers from the state of "Texas"
select  * from mytable where state = "Texas";

 -- Retrieve all records where the csat_score is greater than 5
 select * from mytable where csat_score >5 limit 5;
 
-- Sort the data by call_timestamp in descending order.
select * from mytable order by call_timestamp desc limit 5;

--  Sort the data by response_time and then by call duration in minutes.
select * from mytable order by response_time desc, call_duration_in_minutes desc; 

 -- Count the total number of calls made
 select count(*) as total_num_call from mytable ;
 
--  Find the average csat_score.
select round(avg(csat_score), 1) as avg_score from mytable;

-- Get the minimum and maximum call durations.
select min(call_duration_in_minutes) as min_call_duration,
 max(call_duration_in_minutes) as max_call_duration from mytable;
select * from mytable;
--  Count the number of calls grouped by state
select state, count(*) as num_call from mytable group by  state;

-- Find the average csat_score for each sentiment
select sentiment, round(avg(csat_score),1) as avg_score from mytable
 group by sentiment order by avg_score desc;
 
--   How can you find the total and average call duration for each call center location?
select city, state, avg(call_duration_in_minutes) as avg_call_duration from mytable
group by city, state order by avg_call_duration desc limit 10;

select call_center, sum(call_duration_in_minutes) as total_call_duration,
 avg(call_duration_in_minutes) as avg_call_duration from mytable
group by call_center;

-- Which sentiment category has the highest average CSAT score?
select sentiment, avg(csat_score) as highest_avg_csatscore from mytable group by sentiment;


--  Find the percentage of calls that were resolved within SLA.
select response_time from mytable ;

 select * from mytable limit 5;
 
--  Get all records where csat_score is greater than 5.
select * from mytable where csat_score >5;

-- sort calls by call_duration in descending order.
select  call_duration_in_minutes from mytable order by  call_duration_in_minutes desc;
 select id, customer_name, call_center from mytable order by call_duration_in_minutes desc;
 
--  Calculate the average CSAT score.
select round(avg(csat_score),2) as avg_score from mytable;

-- Count the number of calls per city.
select city, count(*) as numb_call_city from mytable group by city order by numb_call_city desc limit 10;

-- Categorize customers based on their CSAT score: 0-3: Low  4-6: Medium   7-10: High
  select customer_name, csat_score,
  case
  when csat_score between 0 and 3 then 'low'
  when csat_score between 4 and 6 then 'medium'
  else 'high'
end as  satisfaction_level 
  from mytable;
  
  
 select * from mytable; 

-- Rank the calls by call_duration for each state


select csat_score, customer_name, call_duration_in_minutes,
sum( call_duration_in_minutes) over(order by call_duration_in_minutes) as sum,
avg( call_duration_in_minutes) over(order by call_duration_in_minutes) as avrage,
count(call_duration_in_minutes) over(order by call_duration_in_minutes) as count,
min(call_duration_in_minutes) over(order by call_duration_in_minutes) as minimum,
max(call_duration_in_minutes) over(order by call_duration_in_minutes) as maximum from mytable;

select state,
row_number() over(order by  state) as 'row_number',
rank() over(order by  state) as 'rank',
dense_rank() over(order by state) as 'dense_rank',
percent_rank() over(order by state) as 'percent_rank'
from mytable;

select call_duration_in_minutes, 
lead(call_duration_in_minutes,2) over(order by call_duration_in_minutes) as 'lead',
lag(call_duration_in_minutes,2) over(order by call_duration_in_minutes) as 'length'
from mytable;

 

 

