--Num Jobs Per User Per Day
select user, 
creation_date,
count(*) as num_jobs,
avg(total_slot_ms) as avg_slot_ms
from djomniture.bq_analysis.bq_usage
where user = 'newsroom-looker-studio@dj-data-prod-parsely.iam.gserviceaccount.com'
group by user, creation_date
order by creation_date desc