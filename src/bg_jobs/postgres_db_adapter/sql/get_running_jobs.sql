SELECT
    *
FROM
    jobs
WHERE
    reserved_at < CURRENT_TIMESTAMP
    AND reserved_by = $1
