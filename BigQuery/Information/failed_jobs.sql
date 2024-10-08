WITH LongRunningJobs AS (
  SELECT 
    job_id,
    user_email,
    creation_time,
    end_time,
    error_result.message AS failure_reason,  -- Capture failure reason
    total_slot_ms,
    (total_slot_ms / (1000 * 60 * 60)) AS slots_avg_per_hour,  -- Calculate average slots used per hour
    TIMESTAMP_DIFF(end_time, creation_time, SECOND) / 3600.0 AS job_duration_hours,  -- Job duration in hours
    query AS query_string,  -- Capture the query string
    labels,  -- Capture labels (used to identify scheduled queries)
    
    -- Extract scheduled_query_id from labels array
    (SELECT value FROM UNNEST(labels) WHERE key = 'schedule_id') AS scheduled_query_id,
    
    -- Extract trigger_id from labels array
    (SELECT value FROM UNNEST(labels) WHERE key = 'trigger_id') AS trigger_id
    
  FROM 
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT  -- Adjust region as per your setup
  WHERE 
    job_type = 'QUERY'
    AND state = 'DONE'  -- Look for jobs that are marked as DONE
    AND error_result IS NOT NULL  -- Filter for jobs that failed
    AND TIMESTAMP_DIFF(end_time, creation_time, SECOND) / 3600.0 > 6  -- Filter for jobs that ran more than 6 hours
    AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)  -- Optional time range filter
)

SELECT 
  job_id,
  user_email,
  TIMESTAMP_TRUNC(creation_time, HOUR) AS job_hour,  -- Truncate to the hour for hourly grouping
  failure_reason,
  total_slot_ms,
  slots_avg_per_hour,
  job_duration_hours,
  query_string,  -- Include the query string
  scheduled_query_id,  -- Include scheduled query ID
  trigger_id,  -- Include trigger ID (if available)
  labels  -- Include all labels for additional information
FROM 
  LongRunningJobs
ORDER BY 
  creation_time DESC;  -- Order by latest failed jobs first
limit 1
