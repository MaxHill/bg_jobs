UPDATE
    jobs
SET
    attempts = attempts + 1
WHERE
    id = $1
RETURNING
    *;
