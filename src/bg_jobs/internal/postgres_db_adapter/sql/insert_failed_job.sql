INSERT INTO
    jobs_failed (
        id,
        name,
        payload,
        attempts,
        exception,
        created_at,
        available_at,
        failed_at
    )
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING
    *;
