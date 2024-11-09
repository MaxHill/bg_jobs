UPDATE
    jobs
SET
    reserved_at = NULL,
    reserved_by = NULL
WHERE
    id = $1
RETURNING
    *;
