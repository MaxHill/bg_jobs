INSERT INTO
    jobs (
        id,
        name,
        payload,
        attempts,
        created_at,
        available_at
    )
VALUES
    ($1, $2, $3, 0, $4, $5)
RETURNING
    *;
