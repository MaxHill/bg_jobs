import decode
import gleam/option.{type Option}
import gleam/pgo

/// A row you get from running the `release_claim` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/release_claim.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ReleaseClaimRow {
  ReleaseClaimRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: Option(String),
  )
}

/// Runs the `release_claim` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/release_claim.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn release_claim(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use reserved_at <- decode.parameter
      use reserved_by <- decode.parameter
      ReleaseClaimRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        reserved_at: reserved_at,
        reserved_by: reserved_by,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, decode.optional(timestamp_decoder()))
    |> decode.field(7, decode.optional(decode.string))

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
  |> pgo.execute(db, [pgo.text(arg_1)], decode.from(decoder, _))
}

/// A row you get from running the `insert_succeeded_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/insert_succeeded_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertSucceededJobRow {
  InsertSucceededJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    succeeded_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

/// Runs the `insert_succeeded_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/insert_succeeded_job.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn insert_succeeded_job(db, arg_1, arg_2, arg_3, arg_4, arg_5, arg_6, arg_7) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use succeeded_at <- decode.parameter
      InsertSucceededJobRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        succeeded_at: succeeded_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, timestamp_decoder())

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
  |> pgo.execute(
    db,
    [
      pgo.text(arg_1),
      pgo.text(arg_2),
      pgo.text(arg_3),
      pgo.int(arg_4),
      pgo.timestamp(arg_5),
      pgo.timestamp(arg_6),
      pgo.timestamp(arg_7),
    ],
    decode.from(decoder, _),
  )
}

/// A row you get from running the `get_failed_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_failed_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetFailedJobsRow {
  GetFailedJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    failed_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

/// Runs the `get_failed_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_failed_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_failed_jobs(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use exception <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use failed_at <- decode.parameter
      GetFailedJobsRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        exception: exception,
        created_at: created_at,
        available_at: available_at,
        failed_at: failed_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, decode.string)
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, timestamp_decoder())
    |> decode.field(7, timestamp_decoder())

  "SELECT
    *
FROM
    jobs_failed
LIMIT
    $1;
"
  |> pgo.execute(db, [pgo.int(arg_1)], decode.from(decoder, _))
}

/// Runs the `delete_enqueued_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/delete_enqueued_job.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn delete_enqueued_job(db, arg_1) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "DELETE FROM
    jobs
WHERE
    id = $1;
"
  |> pgo.execute(db, [pgo.text(arg_1)], decode.from(decoder, _))
}

/// A row you get from running the `get_succeeded_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_succeeded_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetSucceededJobsRow {
  GetSucceededJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    succeeded_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

/// Runs the `get_succeeded_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_succeeded_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_succeeded_jobs(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use succeeded_at <- decode.parameter
      GetSucceededJobsRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        succeeded_at: succeeded_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, timestamp_decoder())

  "SELECT
    *
FROM
    jobs_succeeded
LIMIT
    $1;
"
  |> pgo.execute(db, [pgo.int(arg_1)], decode.from(decoder, _))
}

/// A row you get from running the `increment_attempts` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/increment_attempts.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type IncrementAttemptsRow {
  IncrementAttemptsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: Option(String),
  )
}

/// Runs the `increment_attempts` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/increment_attempts.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn increment_attempts(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use reserved_at <- decode.parameter
      use reserved_by <- decode.parameter
      IncrementAttemptsRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        reserved_at: reserved_at,
        reserved_by: reserved_by,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, decode.optional(timestamp_decoder()))
    |> decode.field(7, decode.optional(decode.string))

  "UPDATE
    jobs
SET
    attempts = attempts + 1
WHERE
    id = $1
RETURNING
    *;
"
  |> pgo.execute(db, [pgo.text(arg_1)], decode.from(decoder, _))
}

/// A row you get from running the `insert_failed_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/insert_failed_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type InsertFailedJobRow {
  InsertFailedJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    failed_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

/// Runs the `insert_failed_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/insert_failed_job.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
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
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use exception <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use failed_at <- decode.parameter
      InsertFailedJobRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        exception: exception,
        created_at: created_at,
        available_at: available_at,
        failed_at: failed_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, decode.string)
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, timestamp_decoder())
    |> decode.field(7, timestamp_decoder())

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
  |> pgo.execute(
    db,
    [
      pgo.text(arg_1),
      pgo.text(arg_2),
      pgo.text(arg_3),
      pgo.int(arg_4),
      pgo.text(arg_5),
      pgo.timestamp(arg_6),
      pgo.timestamp(arg_7),
      pgo.timestamp(arg_8),
    ],
    decode.from(decoder, _),
  )
}

/// A row you get from running the `enqueue_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/enqueue_job.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type EnqueueJobRow {
  EnqueueJobRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: Option(String),
  )
}

/// Runs the `enqueue_job` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/enqueue_job.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn enqueue_job(db, arg_1, arg_2, arg_3, arg_4, arg_5) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use reserved_at <- decode.parameter
      use reserved_by <- decode.parameter
      EnqueueJobRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        reserved_at: reserved_at,
        reserved_by: reserved_by,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, decode.optional(timestamp_decoder()))
    |> decode.field(7, decode.optional(decode.string))

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
  |> pgo.execute(
    db,
    [
      pgo.text(arg_1),
      pgo.text(arg_2),
      pgo.text(arg_3),
      pgo.timestamp(arg_4),
      pgo.timestamp(arg_5),
    ],
    decode.from(decoder, _),
  )
}

/// A row you get from running the `get_enqueued_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_enqueued_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetEnqueuedJobsRow {
  GetEnqueuedJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: Option(String),
  )
}

/// Runs the `get_enqueued_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_enqueued_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_enqueued_jobs(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use reserved_at <- decode.parameter
      use reserved_by <- decode.parameter
      GetEnqueuedJobsRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        reserved_at: reserved_at,
        reserved_by: reserved_by,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, decode.optional(timestamp_decoder()))
    |> decode.field(7, decode.optional(decode.string))

  "SELECT
    *
FROM
    jobs
WHERE
    name = $1
    AND reserved_at IS NULL
"
  |> pgo.execute(db, [pgo.text(arg_1)], decode.from(decoder, _))
}

/// A row you get from running the `get_running_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_running_jobs.sql`.
///
/// > 🐿️ This type definition was generated automatically using v1.7.1 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetRunningJobsRow {
  GetRunningJobsRow(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: Option(String),
  )
}

/// Runs the `get_running_jobs` query
/// defined in `./src/bg_jobs/internal/postgres_db_adapter/sql/get_running_jobs.sql`.
///
/// > 🐿️ This function was generated automatically using v1.7.1 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_running_jobs(db, arg_1) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use reserved_at <- decode.parameter
      use reserved_by <- decode.parameter
      GetRunningJobsRow(
        id: id,
        name: name,
        payload: payload,
        attempts: attempts,
        created_at: created_at,
        available_at: available_at,
        reserved_at: reserved_at,
        reserved_by: reserved_by,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.int)
    |> decode.field(4, timestamp_decoder())
    |> decode.field(5, timestamp_decoder())
    |> decode.field(6, decode.optional(timestamp_decoder()))
    |> decode.field(7, decode.optional(decode.string))

  "SELECT
    *
FROM
    jobs
WHERE
    reserved_at < CURRENT_TIMESTAMP
    AND reserved_by = $1
"
  |> pgo.execute(db, [pgo.text(arg_1)], decode.from(decoder, _))
}

// --- UTILS -------------------------------------------------------------------

/// A decoder to decode `timestamp`s coming from a Postgres query.
///
fn timestamp_decoder() {
  use dynamic <- decode.then(decode.dynamic)
  case pgo.decode_timestamp(dynamic) {
    Ok(timestamp) -> decode.into(timestamp)
    Error(_) -> decode.fail("timestamp")
  }
}
