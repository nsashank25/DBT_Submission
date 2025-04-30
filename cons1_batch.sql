SELECT 
    batter, 
    SUM(total_runs) AS total_runs_scored
FROM 
    deliveries
GROUP BY 
    batter
ORDER BY 
    total_runs_scored DESC;