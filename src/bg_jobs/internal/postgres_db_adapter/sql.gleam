import gleam/dynamic/decode
import gleam/option.{type Option}
import pog

/// A row you get from running the `release_jobs_reserved_by` query
/// defined in `./src/delete/sql/release_jobs_reserved_by.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReleaseJobsReservedByRow {
  ReleaseJobsReservedByRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `release_jobs_reserved_by` query
/// defined in `./src/delete/sql/release_jobs_reserved_by.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn release_jobs_reserved_by(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      ReleaseJobsReservedByRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "UPDATE
    jobs
SET
    reserved_at = NULL,
    reserved_by = NULL
WHERE
    reserved_by = $1
RETURNING
    *;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `enqueue_job` query
/// defined in `./src/delete/sql/enqueue_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type EnqueueJobRow {
  EnqueueJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `enqueue_job` query
/// defined in `./src/delete/sql/enqueue_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn enqueue_job(db, arg_1, arg_2, arg_3, arg_4, arg_5) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      EnqueueJobRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "INSERT INTO
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.text(arg_3))
  |> pog.parameter(pog.timestamp(arg_4))
  |> pog.parameter(pog.timestamp(arg_5))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_enqueued_jobs` query
/// defined in `./src/delete/sql/get_enqueued_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetEnqueuedJobsRow {
  GetEnqueuedJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `get_enqueued_jobs` query
/// defined in `./src/delete/sql/get_enqueued_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_enqueued_jobs(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      GetEnqueuedJobsRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "SELECT
    *
FROM
    jobs
WHERE
    name = $1
    AND reserved_at IS NULL
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_failed_jobs` query
/// defined in `./src/delete/sql/get_failed_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetFailedJobsRow {
  GetFailedJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    failed_at: pog.Timestamp,
  )
}

/// Runs the `get_failed_jobs` query
/// defined in `./src/delete/sql/get_failed_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_failed_jobs(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use exception <- decode.field(4, decode.string)
    use created_at <- decode.field(5, pog.timestamp_decoder())
    use available_at <- decode.field(6, pog.timestamp_decoder())
    use failed_at <- decode.field(7, pog.timestamp_decoder())
    decode.success(
      GetFailedJobsRow(
        id:,
        name:,
        payload:,
        attempts:,
        exception:,
        created_at:,
        available_at:,
        failed_at:,
      ),
    )
  }

  "SELECT
    *
FROM
    jobs_failed
LIMIT
    $1;
"
  |> pog.query
  |> pog.parameter(pog.int(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `increment_attempts` query
/// defined in `./src/delete/sql/increment_attempts.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type IncrementAttemptsRow {
  IncrementAttemptsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `increment_attempts` query
/// defined in `./src/delete/sql/increment_attempts.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn increment_attempts(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      IncrementAttemptsRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "UPDATE
    jobs
SET
    attempts = attempts + 1
WHERE
    id = $1
RETURNING
    *;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_running_jobs_by_queue_name` query
/// defined in `./src/delete/sql/get_running_jobs_by_queue_name.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetRunningJobsByQueueNameRow {
  GetRunningJobsByQueueNameRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `get_running_jobs_by_queue_name` query
/// defined in `./src/delete/sql/get_running_jobs_by_queue_name.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_running_jobs_by_queue_name(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      GetRunningJobsByQueueNameRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "SELECT
    *
FROM
    jobs
WHERE
    reserved_at <= CURRENT_TIMESTAMP
    AND reserved_by = $1
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `delete_enqueued_job` query
/// defined in `./src/delete/sql/delete_enqueued_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn delete_enqueued_job(db, arg_1) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "DELETE FROM
    jobs
WHERE
    id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `insert_failed_job` query
/// defined in `./src/delete/sql/insert_failed_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertFailedJobRow {
  InsertFailedJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    failed_at: pog.Timestamp,
  )
}

/// Runs the `insert_failed_job` query
/// defined in `./src/delete/sql/insert_failed_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn insert_failed_job(
  db,
  arg_1,
  arg_2,
  arg_3,
  arg_4,
  arg_5,
  arg_6,
  arg_7,
  arg_8,
) {
  let decoder =
  {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use exception <- decode.field(4, decode.string)
    use created_at <- decode.field(5, pog.timestamp_decoder())
    use available_at <- decode.field(6, pog.timestamp_decoder())
    use failed_at <- decode.field(7, pog.timestamp_decoder())
    decode.success(
      InsertFailedJobRow(
        id:,
        name:,
        payload:,
        attempts:,
        exception:,
        created_at:,
        available_at:,
        failed_at:,
      ),
    )
  }

  "INSERT INTO
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.text(arg_3))
  |> pog.parameter(pog.int(arg_4))
  |> pog.parameter(pog.text(arg_5))
  |> pog.parameter(pog.timestamp(arg_6))
  |> pog.parameter(pog.timestamp(arg_7))
  |> pog.parameter(pog.timestamp(arg_8))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `release_reservation` query
/// defined in `./src/delete/sql/release_reservation.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReleaseReservationRow {
  ReleaseReservationRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `release_reservation` query
/// defined in `./src/delete/sql/release_reservation.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn release_reservation(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      ReleaseReservationRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "UPDATE
    jobs
SET
    reserved_at = NULL,
    reserved_by = NULL
WHERE
    id = $1
RETURNING
    *;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `insert_succeeded_job` query
/// defined in `./src/delete/sql/insert_succeeded_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertSucceededJobRow {
  InsertSucceededJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    succeeded_at: pog.Timestamp,
  )
}

/// Runs the `insert_succeeded_job` query
/// defined in `./src/delete/sql/insert_succeeded_job.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn insert_succeeded_job(db, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6, arg_7,
) {
  let decoder =
  {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use succeeded_at <- decode.field(6, pog.timestamp_decoder())
    decode.success(
      InsertSucceededJobRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        succeeded_at:,
      ),
    )
  }

  "INSERT INTO
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
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.text(arg_3))
  |> pog.parameter(pog.int(arg_4))
  |> pog.parameter(pog.timestamp(arg_5))
  |> pog.parameter(pog.timestamp(arg_6))
  |> pog.parameter(pog.timestamp(arg_7))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_succeeded_jobs` query
/// defined in `./src/delete/sql/get_succeeded_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetSucceededJobsRow {
  GetSucceededJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    succeeded_at: pog.Timestamp,
  )
}

/// Runs the `get_succeeded_jobs` query
/// defined in `./src/delete/sql/get_succeeded_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_succeeded_jobs(db, arg_1) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use succeeded_at <- decode.field(6, pog.timestamp_decoder())
    decode.success(
      GetSucceededJobsRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        succeeded_at:,
      ),
    )
  }

  "SELECT
    *
FROM
    jobs_succeeded
LIMIT
    $1;
"
  |> pog.query
  |> pog.parameter(pog.int(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_running_jobs` query
/// defined in `./src/delete/sql/get_running_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v3.0.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetRunningJobsRow {
  GetRunningJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: pog.Timestamp,
    available_at: pog.Timestamp,
    reserved_at: Option(pog.Timestamp),
    reserved_by: Option(String),
  )
}

/// Runs the `get_running_jobs` query
/// defined in `./src/delete/sql/get_running_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v3.0.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_running_jobs(db) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use name <- decode.field(1, decode.string)
    use payload <- decode.field(2, decode.string)
    use attempts <- decode.field(3, decode.int)
    use created_at <- decode.field(4, pog.timestamp_decoder())
    use available_at <- decode.field(5, pog.timestamp_decoder())
    use reserved_at <- decode.field(6, decode.optional(pog.timestamp_decoder()))
    use reserved_by <- decode.field(7, decode.optional(decode.string))
    decode.success(
      GetRunningJobsRow(
        id:,
        name:,
        payload:,
        attempts:,
        created_at:,
        available_at:,
        reserved_at:,
        reserved_by:,
      ),
    )
  }

  "SELECT
    *
FROM
    jobs
WHERE
    reserved_by IS NOT NULL
"
  |> pog.query
  |> pog.returning(decoder)
  |> pog.execute(db)
}
