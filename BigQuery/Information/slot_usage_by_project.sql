WITH daily_slot_usage as (
  SELECT creation_date,
  SUM(total_slot_ms),
  SUM(total_bytes_processed),
  SUM(total_bytes_processed) / 1073741824.0 as total_gb_processed,
  SUM(total_slot_ms) / (1000 * 60 * 60) AS slot_hours,
  SUM(job_duration_ms),
  project_id
  from djomniture.bq_analysis.bq_usage
  where creation_date between '2024-07-01' and '2024-09-13'
  group by creation_date, project_id
)
SELECT
project_id,
creation_date,
count(*) as num_jobs,
slot_hours,
slot_hours / 24 AS slot_days,
from daily_slot_usage
where creation_date between '2024-07-11' and '2024-09-13'
and project_id in (
  'dj-data-nonprod-parsely', 
  'dj-data-stag-parsely', 
  'dj-data-prod-parsely',
  'dj-users',
  'djomniture',
  'dj-data-prod-ga4')
group by project_id, creation_date, slot_days, slot_hours
order by creation_date desc
