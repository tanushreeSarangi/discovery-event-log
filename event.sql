Select * from event_log;
create table event_log_2
(
uuid character varying,	
timestamp	integer,
session_id character varying,
"group" character varying,	
"action" character varying,	
checkin	integer,
page_id	character varying,	
n_results integer,
result_position	integer)

ALTER TABLE event_log_2
ALTER COLUMN timestamp TYPE character varying

ALTER TABLE event_log_2
ALTER COLUMN n_results TYPE character varying

select * from event_log_2 where action = 'searchResultPage' and n_results = '0' limit 10 "760bf89817ce4b08" "30"

update event_log_2
set n_results = null
where n_results = 'NA'

select Left("timestamp",8) from event_log_2 limit 10
--Assuming that the actions - visitPage and checkin will always have a result_position associated with them. Hence, removing the records
--where the action is VisitPage or checkin and the result_position is null
--Assuming that the timestamp will always have a numeric value. Hence, removing the records where the timestamp column has special characters or alpha numeric value.
--For zero yield we have to take in account both where the n_results are null and where the n_results are 0.
select * from event_log_2 where result_position <> 'NA' limit 10

WITH CTE_searchResults as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_searches from event_log_2 
where action = 'searchResultPage' 
group by "group", Left("timestamp",8)
)
, CTE_visitPages as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_visits from event_log_2 
where action = 'visitPage' and result_position <> 'NA'
group by "group", Left("timestamp",8)
)
,CTE_zeroYield as
(
select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_zeroyields from event_log_2 
where action = 'searchResultPage' and (n_results = '0' or n_results is null)
group by "group", Left("timestamp",8)
)
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
select a.datepart_timestamp, a."group", cast(total_visits*100/total_searches as float) as clickthroughrate, 
(total_zeroyields*100/total_searches) as zeroyield,
d.result_position
from CTE_searchResults a
left join CTE_visitPages b on a.datepart_timestamp = b.datepart_timestamp and a."group" = b."group"
left join CTE_zeroYield c on a.datepart_timestamp = c.datepart_timestamp and a."group" = c."group"
left join CTE_count_resultPostion d on a.datepart_timestamp = d.datepart_timestamp and a."group" = d."group"
where d.ranking = 1
order by a.datepart_timestamp



select * from event_log_2 where session_id = 'b549a21cf5f272e2'
action = 'visitPage' and result_position = 'NA'

select * from event_log_2 where Left("timestamp",8) ilike '%2.%'

select Left("timestamp",8) as datepart_timestamp, "group",count(distinct session_id) as total_searches from event_log_2 
where action = 'searchResultPage' 
group by "group", Left("timestamp",8) order by Left("timestamp",8)

with cte_max_checkin as
(
select max(cast(checkin as int)) as checkin, session_id,page_id, Left("timestamp",8) as datepart_timestamp from event_log_2 
where action = 'checkin' --and session_id = '426b242692d66473'
group by session_id,page_id,Left("timestamp",8)
order by Left("timestamp",8)
)
select datepart_timestamp, session_id, sum(checkin) 
from cte_max_checkin
group by session_id, datepart_timestamp
order by datepart_timestamp

select * from event_log_2 where session_id = 'aa89be8089ff5694' order by timestamp asc
--"13cd6d70d0fa2b58"

