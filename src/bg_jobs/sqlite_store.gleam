import bg_jobs/internal/utils
import bg_jobs/job
import bg_jobs/queue
import birl
import decode
import gleam/dynamic
import gleam/list
import gleam/option
import gleam/result
import sqlight
import youid/uuid

pub fn try_new_store(conn: sqlight.Connection) {
  queue.JobStore(
    enqueue_job: enqueue_job(conn),
    get_next_jobs: get_next_jobs(conn),
    move_job_to_succeded: move_job_to_succeded(conn),
    move_job_to_failed: move_job_to_failed(conn),
    get_succeeded_jobs: get_succeeded_jobs(conn),
    get_failed_jobs: get_failed_jobs(conn),
    increment_attempts: increment_attempts(conn),
  )
}

pub fn move_job_to_succeded(conn: sqlight.Connection) {
  fn(job: job.Job) {
    use _ <- result.try(
      sqlight.query(
        "DELETE FROM jobs
         WHERE id =  ?;",
        conn,
        [sqlight.text(job.id)],
        utils.discard_decode,
      )
      |> result.map_error(queue.DbError),
    )

    sqlight.query(
      "
      INSERT INTO jobs_succeeded (
        id, 
        queue, 
        name, 
        payload, 
        attempts, 
        created_at, 
        available_at, 
        succeded_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      RETURNING *;
    ",
      conn,
      [
        sqlight.text(job.id),
        sqlight.text(job.queue),
        sqlight.text(job.name),
        sqlight.text(job.payload),
        sqlight.int(job.attempts),
        sqlight.text(
          birl.to_iso8601(birl.from_erlang_universal_datetime(job.created_at)),
        ),
        sqlight.text(
          birl.to_iso8601(birl.from_erlang_universal_datetime(job.available_at)),
        ),
      ],
      decode_succeded_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.replace(Nil)
  }
}

pub fn get_succeeded_jobs(conn: sqlight.Connection) {
  fn(limit: Int) {
    sqlight.query(
      "
      SELECT * FROM jobs_succeeded
      LIMIT ?;
    ",
      conn,
      [sqlight.int(limit)],
      decode_succeded_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.map(result.all)
    |> result.flatten
  }
}

pub fn get_failed_jobs(conn: sqlight.Connection) {
  fn(limit: Int) {
    sqlight.query(
      "
      SELECT * FROM jobs_failed
      LIMIT ?;
    ",
      conn,
      [sqlight.int(limit)],
      decode_failed_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.map(result.all)
    |> result.flatten
  }
}

pub fn move_job_to_failed(conn: sqlight.Connection) {
  fn(job: job.Job, exception: String) {
    let sql =
      "DELETE FROM jobs
      WHERE id =  ?;"

    use _ <- result.try(
      sqlight.query(sql, conn, [sqlight.text(job.id)], utils.discard_decode)
      |> result.map_error(queue.DbError),
    )

    let sql =
      "
      INSERT INTO jobs_failed (
        id, 
        queue, 
        name, 
        payload, 
        attempts, 
        exception,
        created_at, 
        available_at, 
        failed_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      RETURNING *;
    "

    sqlight.query(
      sql,
      conn,
      [
        sqlight.text(job.id),
        sqlight.text(job.queue),
        sqlight.text(job.name),
        sqlight.text(job.payload),
        sqlight.int(job.attempts),
        sqlight.text(exception),
        sqlight.text(
          birl.to_iso8601(birl.from_erlang_universal_datetime(job.created_at)),
        ),
        sqlight.text(
          birl.to_iso8601(birl.from_erlang_universal_datetime(job.available_at)),
        ),
      ],
      decode_failed_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.replace(Nil)
  }
}

pub fn get_next_jobs(conn: sqlight.Connection) {
  fn(queue_name: String, limit: Int) {
    let sql =
      "
        UPDATE jobs
           SET reserved_at = CURRENT_TIMESTAMP
         WHERE id = (
          SELECT id
            FROM jobs
           WHERE queue = ?
             AND available_at <= CURRENT_TIMESTAMP  
             AND reserved_at IS NULL  
        ORDER BY available_at ASC  
           LIMIT ?
      )
      RETURNING *;
    "

    sqlight.query(
      sql,
      conn,
      [sqlight.text(queue_name), sqlight.int(limit)],
      decode_enqueued_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.map(result.all)
    |> result.flatten
  }
}

pub fn increment_attempts(conn: sqlight.Connection) {
  fn(job: job.Job) {
    sqlight.query(
      "
    UPDATE jobs
       SET attempts = attempts + 1
     WHERE id = ?
    RETURNING *
  ",
      conn,
      [sqlight.text(job.id)],
      decode_enqueued_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.try(fn(list) {
      list.first(list)
      |> result.replace_error(queue.Unknown(
        "Should never be possible if update is successfull",
      ))
    })
    |> result.flatten
  }
}

pub fn enqueue_job(conn: sqlight.Connection) {
  fn(queue_name: String, job_name: String, payload: String) {
    let sql =
      "
      INSERT INTO jobs (id, queue, name, payload, attempts, created_at, available_at, reserved_at)
      VALUES ($1, $2, $3, $4, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)
      RETURNING *;
    "

    sqlight.query(
      sql,
      conn,
      [
        sqlight.text(uuid.v4_string()),
        sqlight.text(queue_name),
        sqlight.text(job_name),
        sqlight.text(payload),
      ],
      decode_enqueued_db_row,
    )
    |> result.map_error(queue.DbError)
    |> result.try(fn(list) {
      list.first(list)
      |> result.replace_error(queue.Unknown(
        "Should never be possible if insert is successfull",
      ))
    })
    |> result.flatten
  }
}

pub fn decode_enqueued_db_row(data: dynamic.Dynamic) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use queue <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at_string <- decode.parameter
      use available_at_string <- decode.parameter
      use reserved_at_string <- decode.parameter

      use created_at <- result.try(
        birl.from_naive(created_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )
      use available_at <- result.try(
        birl.from_naive(available_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )

      use reserved_at <- result.map(
        option.map(reserved_at_string, fn(dt) {
          birl.from_naive(dt)
          |> result.map(birl.to_erlang_universal_datetime)
          |> result.replace_error(queue.ParseDateError(dt))
        })
        |> utils.transpose_option_to_result,
      )

      job.Job(
        id,
        queue,
        name,
        payload,
        attempts,
        created_at,
        available_at,
        reserved_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.string)
    |> decode.field(4, decode.int)
    |> decode.field(5, decode.string)
    |> decode.field(6, decode.string)
    |> decode.field(7, decode.optional(decode.string))

  decoder
  |> decode.from(data)
}

pub fn decode_succeded_db_row(data: dynamic.Dynamic) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use queue <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at_string <- decode.parameter
      use available_at_string <- decode.parameter
      use succeded_at_string <- decode.parameter

      use created_at <- result.try(
        birl.from_naive(created_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )

      use available_at <- result.try(
        birl.from_naive(available_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )

      use succeded_at <- result.map(
        birl.from_naive(succeded_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(succeded_at_string)),
      )

      job.SucceededJob(
        id,
        queue,
        name,
        payload,
        attempts,
        created_at,
        available_at,
        succeded_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.string)
    |> decode.field(4, decode.int)
    |> decode.field(5, decode.string)
    |> decode.field(6, decode.string)
    |> decode.field(7, decode.string)

  decoder
  |> decode.from(data)
}

pub fn decode_failed_db_row(data: dynamic.Dynamic) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use queue <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use exception <- decode.parameter
      use failed_at_string <- decode.parameter
      use created_at_string <- decode.parameter
      use available_at_string <- decode.parameter

      use created_at <- result.try(
        birl.from_naive(created_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )

      use available_at <- result.try(
        birl.from_naive(available_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(created_at_string)),
      )

      use failed_at <- result.map(
        birl.from_naive(failed_at_string)
        |> result.map(birl.to_erlang_universal_datetime)
        |> result.replace_error(queue.ParseDateError(failed_at_string)),
      )

      job.FailedJob(
        id,
        queue,
        name,
        payload,
        attempts,
        exception,
        created_at,
        available_at,
        failed_at,
      )
    })
    |> decode.field(0, decode.string)
    |> decode.field(1, decode.string)
    |> decode.field(2, decode.string)
    |> decode.field(3, decode.string)
    |> decode.field(4, decode.int)
    |> decode.field(5, decode.string)
    |> decode.field(6, decode.string)
    |> decode.field(7, decode.string)
    |> decode.field(8, decode.string)

  decoder
  |> decode.from(data)
}
