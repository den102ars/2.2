DELETE FROM dm.client
WHERE ctid IN (
    SELECT ctid
    FROM (
        SELECT 
            ctid,
            ROW_NUMBER() OVER (
                PARTITION BY client_rk, effective_from_date 
                ORDER BY ctid
            ) AS row_num
        FROM dm.client
    ) AS duplicates
    WHERE row_num > 1
);


SELECT client_rk, effective_from_date, COUNT(*)
FROM dm.client
GROUP BY client_rk, effective_from_date
HAVING COUNT(*) > 1;