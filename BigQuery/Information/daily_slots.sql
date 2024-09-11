WITH daily_slot_usage as (
  SELECT creation_date,
  SUM(total_slot_ms) / (1000 * 60 * 60) AS slot_hours,
  project_id
  from djomniture.bq_analysis.bq_usage
  where creation_date between '2024-08-11' and '2024-09-11'
  group by creation_date, project_id
)
SELECT
project_id,
creation_date,
count(*) as num_jobs,
slot_hours,
slot_hours / 24 AS slot_days
from daily_slot_usage
where creation_date between '2024-08-11' and '2024-09-11'
group by project_id, creation_date, slot_days, slot_hours
order by creation_date desc
