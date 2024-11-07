SELECT
    *
FROM
    jobs
WHERE
    name = $1
    AND reserved_at IS NULL
