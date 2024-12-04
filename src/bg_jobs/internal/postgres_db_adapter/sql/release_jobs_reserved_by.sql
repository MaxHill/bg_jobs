UPDATE
    jobs
SET
    reserved_at = NULL,
    reserved_by = NULL
WHERE
    reserved_by = $1
RETURNING
    *;
