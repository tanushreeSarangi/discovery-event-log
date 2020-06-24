--create the table to fill the event_log data with
create table event_log_2
(
uuid character varying,	
timestamp	character varying,
session_id character varying,
"group" character varying,	
"action" character varying,	
checkin	character varying,
page_id	character varying,	
n_results character varying,
result_position	character varying)

--import the csv data into this table.

update event_log_2
set n_results = null
where n_results = 'NA'

--this CTE(Common Table Expression) counts the unique session_ids for searchResultPage for all dates and groups
WITH CTE_searchResults as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_searches from event_log_2 
where action = 'searchResultPage' 
group by "group", Left("timestamp",8)
)
--this CTE counts the unique session_ids for visitPage for all dates and groups
, CTE_visitPages as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_visits from event_log_2 
where action = 'visitPage' and result_position <> 'NA'
group by "group", Left("timestamp",8)
)
--this CTE counts the unique session_ids for searchResultPage which yielded 0 or null results, for all dates and groups
,CTE_zeroYield as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_zeroyields from event_log_2 
where action = 'searchResultPage' and (n_results = '0' or n_results is null)
group by "group", Left("timestamp",8)
)
--count all the result_postions for all dates and sessions, ranking them according to their count (max count --> rank 1, 2nd most count -->rank 2...)
,CTE_count_resultPostion as
(
select Left("timestamp",8) as datepart_timestamp, "group", 
	count(result_position) as count_result_position, 
	result_position
	,RANK() OVER (partition by Left("timestamp",8), "group" Order BY COUNT(result_position) DESC) as ranking
from event_log_2 
where action = 'visitPage' and result_position <> 'NA'
group by "group", Left("timestamp",8), result_position
)
--Join all the CTEs together
--left joining on the CTE_searchResults as the workflow starts from searching and the other actions cannot be performed without it
--filtering only for the Rank 1 counts to get the max clicked resuklt position for everyday per group
select a.datepart_timestamp, a."group", cast(total_visits*100/total_searches as float) as clickthroughrate, 
(total_zeroyields*100/total_searches) as zeroyield,
d.result_position
from CTE_searchResults a
left join CTE_visitPages b on a.datepart_timestamp = b.datepart_timestamp and a."group" = b."group"
left join CTE_zeroYield c on a.datepart_timestamp = c.datepart_timestamp and a."group" = c."group"
left join CTE_count_resultPostion d on a.datepart_timestamp = d.datepart_timestamp and a."group" = d."group"
where d.ranking = 1
order by a.datepart_timestamp

--This cte gets the session_length
--finding the max checkin time for each session and then summing it up (as one session can have different pages with different checkins)
--grouping by the date, session and group
with cte_max_checkin as
(
select max(cast(checkin as int)) as checkin, session_id,page_id, Left("timestamp",8) as datepart_timestamp from event_log_2 
where action = 'checkin'
group by session_id,page_id,Left("timestamp",8)
order by Left("timestamp",8)
)
select datepart_timestamp, session_id, sum(checkin) 
from cte_max_checkin
group by session_id, datepart_timestamp
order by datepart_timestamp


