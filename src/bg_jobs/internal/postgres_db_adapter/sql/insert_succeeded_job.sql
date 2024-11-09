INSERT INTO
    jobs_succeeded (
        id,
        name,
        payload,
        attempts,
        created_at,
        available_at,
        succeeded_at
    )
VALUES
    ($1, $2, $3, $4, $5, $6, $7)
RETURNING
    *;
