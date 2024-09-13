SELECT 
    COUNT(*) AS total_queries, 
    SUM(data_scanned_in_bytes) / 1e9 AS total_data_scanned_gb, 
    SUM(total_execution_time_in_millis) / 1000 AS total_execution_time_secs,
    AVG(data_scanned_in_bytes) / 1e9 AS avg_data_scanned_gb,
    AVG(total_execution_time_in_millis) / 1000 AS avg_execution_time_secs,
    SUM(engine_execution_time_in_millis) / 1000 AS total_engine_execution_time_secs,
    AVG(engine_execution_time_in_millis) / 1000 AS avg_engine_execution_time_secs,
    SUM(query_queue_time_in_millis) / 1000 AS total_queue_time_secs,
    AVG(query_queue_time_in_millis) / 1000 AS avg_queue_time_secs,
    COUNT(CASE WHEN state = 'FAILED' THEN 1 END) AS failed_queries,
    
    -- Calculate cost based on data scanned. Athena charges $5 per TB scanned
    SUM(data_scanned_in_bytes / 1e12 * 5) AS total_cost_usd,
    AVG(data_scanned_in_bytes / 1e12 * 5) AS avg_cost_per_query_usd

FROM 
    query_executions
WHERE 
    cycle_date BETWEEN DATE('2024-07-01') AND DATE('2024-09-13') -- Add appropriate date range
GROUP BY 
    cycle_date
ORDER BY
    cycle_date desc ;
