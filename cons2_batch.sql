SELECT 
    bowling_team AS team,
    COUNT(*) AS balls_bowled
FROM deliveries
GROUP BY bowling_team
ORDER BY balls_bowled DESC;