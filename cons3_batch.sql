SELECT 
    bowler, 
    COUNT(*) as wickets_taken
FROM wickets
WHERE dismissal_kind != 'run out'
GROUP BY bowler
ORDER BY wickets_taken DESC;