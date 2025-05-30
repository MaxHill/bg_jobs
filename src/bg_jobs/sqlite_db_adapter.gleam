import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/utils
import bg_jobs/jobs
import gleam/dynamic/decode
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import sqlight
import tempo
import tempo/naive_datetime
import youid/uuid

/// Create a new sqlite_db_adapter
///
pub fn new(conn: sqlight.Connection, event_listners: List(events.EventListener)) {
  let send_event = events.send_event(event_listners, _)
  db_adapter.DbAdapter(
    enqueue_job: enqueue_job(conn, send_event),
    reserve_jobs: reserve_jobs(conn, send_event),
    release_reservation: release_reservation(conn, send_event),
    release_jobs_reserved_by: release_jobs_reserved_by(conn, send_event),
    move_job_to_succeeded: move_job_to_succeeded(conn, send_event),
    move_job_to_failed: move_job_to_failed(conn, send_event),
    increment_attempts: increment_attempts(conn, send_event),
    get_enqueued_jobs: get_enqueued_jobs(conn, send_event),
    get_running_jobs: get_running_jobs(conn, send_event),
    get_running_jobs_by_queue_name: get_running_jobs_by_queue_name(
      conn,
      send_event,
    ),
    get_succeeded_jobs: get_succeeded_jobs(conn, send_event),
    get_failed_jobs: get_failed_jobs(conn, send_event),
    migrate_up: migrate_up(conn),
    migrate_down: migrate_down(conn),
  )
}

// Wrapping sqlight.query adding error casting and logging
fn query(
  send_event: fn(events.Event) -> Nil,
  sql: String,
  on connection: sqlight.Connection,
  with arguments: List(sqlight.Value),
  expecting decoder: decode.Decoder(t),
) {
  let res =
    sqlight.query(sql, connection, arguments, decoder)
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })

  res
}

/// If a job succeeds within the retries, this function
/// will be called and move the job to the succeeded jobs table
///
fn move_job_to_succeeded(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(job: jobs.Job) -> Result(Nil, errors.BgJobError) {
    send_event(events.DbEvent("move_job_to_succeeded", [string.inspect(job)]))
    use _ <- result.try(query(
      send_event,
      "DELETE FROM jobs
         WHERE id =  ?;",
      conn,
      [sqlight.text(job.id)],
      decode.success(Nil),
    ))

    use created_at <- result.try(utils.from_tuple(job.created_at))
    use avaliable_at <- result.try(utils.from_tuple(job.created_at))

    query(
      send_event,
      "
      INSERT INTO jobs_succeeded (
        id,
        name,
        payload,
        attempts,
        created_at,
        available_at,
        succeeded_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
      RETURNING *;
    ",
      conn,
      [
        sqlight.text(job.id),
        sqlight.text(job.name),
        sqlight.text(job.payload),
        sqlight.int(job.attempts),
        sqlight.text(created_at |> to_db_date()),
        sqlight.text(avaliable_at |> to_db_date()),
        sqlight.text(naive_datetime.now_utc() |> to_db_date()),
      ],
      decode_succeeded_db_row(),
    )
    |> result.replace(Nil)
  }
}

/// Get all completed and succeeded jobs
///
fn get_succeeded_jobs(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_succeeded_jobs", [int.to_string(limit)]))
    query(
      send_event,
      "
      SELECT *
      FROM jobs_succeeded
      LIMIT ?;
    ",
      conn,
      [sqlight.int(limit)],
      decode_succeeded_db_row(),
    )
  }
}

/// Get all failed jobs.
///
fn get_failed_jobs(conn: sqlight.Connection, send_event: events.EventListener) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_failed_jobs", [int.to_string(limit)]))
    query(
      send_event,
      "
      SELECT *
      FROM jobs_failed
      LIMIT ?;
    ",
      conn,
      [sqlight.int(limit)],
      decode_failed_db_row(),
    )
  }
}

/// Sets claimed_at and claimed_by and return the job to be processed.
///
fn move_job_to_failed(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(job: jobs.Job, exception: String) {
    send_event(
      events.DbEvent("move_job_to_failed", [string.inspect(job), exception]),
    )
    let sql =
      "DELETE FROM jobs
      WHERE id =  ?;"

    use _ <- result.try(query(
      send_event,
      sql,
      conn,
      [sqlight.text(job.id)],
      decode.success(Nil),
    ))

    use created_at <- result.try(utils.from_tuple(job.created_at))
    use avaliable_at <- result.try(utils.from_tuple(job.created_at))

    let sql =
      "
      INSERT INTO jobs_failed (
        id,
        name,
        payload,
        attempts,
        exception,
        created_at,
        available_at,
        failed_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      RETURNING *;
    "

    query(
      send_event,
      sql,
      conn,
      [
        sqlight.text(job.id),
        sqlight.text(job.name),
        sqlight.text(job.payload),
        sqlight.int(job.attempts),
        sqlight.text(exception),
        sqlight.text(created_at |> to_db_date()),
        sqlight.text(avaliable_at |> to_db_date()),
        sqlight.text(naive_datetime.now_utc() |> to_db_date()),
      ],
      decode_failed_db_row(),
    )
    |> result.replace(Nil)
  }
}

/// Sets claimed_at and claimed_by and return the job to be processed.
///
fn reserve_jobs(conn: sqlight.Connection, send_event: events.EventListener) {
  fn(job_names: List(String), limit: Int, queue_id: String) {
    send_event(events.DbEvent("claim_jobs", [string.inspect(job_names)]))
    let now =
      naive_datetime.now_utc()
      |> to_db_date()

    let job_names_sql =
      list.fold(job_names, "", fn(acc, _) {
        list.filter([acc, "?"], fn(l) { !string.is_empty(l) })
        |> string.join(with: ",")
      })

    let sql = "
        UPDATE jobs
           SET reserved_at = ?, reserved_by = ?
         WHERE id IN (
          SELECT id
            FROM jobs
           WHERE name IN (" <> job_names_sql <> ")
             AND available_at <= ?
             AND reserved_at IS NULL
        ORDER BY available_at ASC
           LIMIT ?
      )
      RETURNING *;
    "
    let arguments =
      list.flatten([
        [sqlight.text(now), sqlight.text(queue_id)],
        list.map(job_names, sqlight.text),
        [sqlight.text(now), sqlight.int(limit)],
      ])

    query(send_event, sql, conn, arguments, decode_enqueued_db_row())
  }
}

/// Release claim from job to allow another queue to process it
///
fn release_reservation(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(job_id: String) {
    send_event(events.DbEvent("claim_jobs", [job_id]))
    let sql =
      "
        UPDATE jobs
           SET reserved_at = NULL, reserved_by = NULL
         WHERE id = ?
     RETURNING *;
    "

    query(
      send_event,
      sql,
      conn,
      [sqlight.text(job_id)],
      decode_enqueued_db_row(),
    )
    |> result.try(fn(list) {
      list.first(list)
      |> result.replace_error(errors.UnknownError(
        "Should never be possible if insert is successfull",
      ))
    })
  }
}

/// Release claim from job that are clamied by a queue, to allow another queue to process it
///
fn release_jobs_reserved_by(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(reserved_by: String) {
    send_event(events.DbEvent("release_jobs_claimed_by", [reserved_by]))
    let sql =
      "
        UPDATE jobs
           SET reserved_at = NULL, reserved_by = NULL
         WHERE reserved_by = ?
     RETURNING *;
    "

    query(
      send_event,
      sql,
      conn,
      [sqlight.text(reserved_by)],
      decode_enqueued_db_row(),
    )
  }
}

/// Get a specific queues running jobs
///
fn get_running_jobs_by_queue_name(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(queue_id: String) {
    send_event(events.DbEvent("get_running_jobs", [queue_id]))
    query(
      send_event,
      "SELECT * FROM jobs WHERE reserved_at <= ? AND reserved_by = ?",
      conn,
      [
        sqlight.text(naive_datetime.now_utc() |> to_db_date()),
        sqlight.text(queue_id),
      ],
      decode_enqueued_db_row(),
    )
  }
}

/// Get jobs from the database that has not been picked up by any queue
///
fn get_enqueued_jobs(conn: sqlight.Connection, send_event: events.EventListener) {
  fn(job_name: String) {
    send_event(events.DbEvent("get_enqueued_jobs", [job_name]))
    query(
      send_event,
      "SELECT * FROM jobs WHERE name = ? AND reserved_at is NULL",
      conn,
      [sqlight.text(job_name)],
      decode_enqueued_db_row(),
    )
  }
}

/// Get jobs from the database that has not been picked up by any queue
///
fn get_running_jobs(conn: sqlight.Connection, send_event: events.EventListener) {
  fn() {
    send_event(events.DbEvent("get_running_jobs", []))
    query(
      send_event,
      "SELECT * FROM jobs WHERE reserved_at IS NOT NULL",
      conn,
      [],
      decode_enqueued_db_row(),
    )
  }
}

/// If a job processing attempt has failed this should be called to increment the attempts
///
fn increment_attempts(
  conn: sqlight.Connection,
  send_event: events.EventListener,
) {
  fn(job: jobs.Job) {
    send_event(events.DbEvent("increment_attempts", [job.id, job.name]))
    query(
      send_event,
      "
    UPDATE jobs
       SET attempts = attempts + 1
     WHERE id = ?
    RETURNING *
  ",
      conn,
      [sqlight.text(job.id)],
      decode_enqueued_db_row(),
    )
    |> result.try(fn(list) {
      list.first(list)
      |> result.replace_error(errors.UnknownError(
        "Should never be possible if update is successfull",
      ))
    })
  }
}

/// Enqueues a job for queues to claim an process
///
fn enqueue_job(conn: sqlight.Connection, send_event: events.EventListener) {
  fn(
    job_name: String,
    payload: String,
    avaliable_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  ) {
    use available_at_string <- result.try(
      utils.from_tuple(avaliable_at) |> result.map(to_db_date),
    )
    send_event(
      events.DbEvent("enqueue_job", [job_name, payload, available_at_string]),
    )

    let sql =
      "
      INSERT INTO jobs (
        id,
        name,
        payload,
        attempts,
        created_at,
        available_at)
      VALUES (?, ?, ?, 0, ?, ?)
      RETURNING *;
    "

    query(
      send_event,
      sql,
      conn,
      [
        sqlight.text(uuid.v4_string()),
        sqlight.text(job_name),
        sqlight.text(payload),
        sqlight.text(naive_datetime.now_utc() |> to_db_date()),
        sqlight.text(available_at_string),
      ],
      decode_enqueued_db_row(),
    )
    |> result.try(fn(list) {
      list.first(list)
      |> result.replace_error(errors.UnknownError(
        "Should never be possible if insert is successfull",
      ))
    })
  }
}

@internal
pub fn migrate_up(conn: sqlight.Connection) {
  fn(event_listeners: List(events.EventListener)) {
    let sql =
      "
    CREATE TABLE IF NOT EXISTS jobs (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      created_at DATETIME NOT NULL,
      available_at DATETIME NOT NULL,
      reserved_at DATETIME,
      reserved_by TEXT
    );

    CREATE TABLE IF NOT EXISTS jobs_failed (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      exception VARCHAR NOT NULL,
      created_at DATETIME NOT NULL,
      available_at DATETIME NOT NULL,
      failed_at DATETIME NOT NULL
    );

    CREATE TABLE IF NOT EXISTS jobs_succeeded (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL,
      attempts INTEGER NOT NULL,
      created_at DATETIME NOT NULL,
      available_at DATETIME NOT NULL,
      succeeded_at DATETIME NOT NULL
    );
    "

    sqlight.exec(sql, conn)
    |> result.map(fn(_) {
      events.send_event(event_listeners, events.MigrateUpComplete)
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
  }
}

@internal
pub fn migrate_down(conn: sqlight.Connection) {
  fn(event_listeners: List(events.EventListener)) {
    let sql =
      "DROP TABLE IF EXISTS jobs;
    DROP TABLE IF EXISTS jobs_failed;
    DROP TABLE IF EXISTS jobs_succeeded;"

    sqlight.exec(sql, conn)
    |> result.map(fn(_) {
      events.send_event(event_listeners, events.MigrateDownComplete)
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
  }
}

// Decode
//---------------

@internal
pub fn decode_enqueued_db_row() -> decode.Decoder(jobs.Job) {
  use id <- decode.field(0, decode.string)
  use name <- decode.field(1, decode.string)
  use payload <- decode.field(2, decode.string)
  use attempts <- decode.field(3, decode.int)
  use created_at <- decode.field(4, timestamp_decoder())
  use available_at <- decode.field(5, timestamp_decoder())
  use reserved_at <- decode.field(6, decode.optional(timestamp_decoder()))
  use reserved_by <- decode.field(7, decode.optional(decode.string))
  decode.success(jobs.Job(
    id:,
    name:,
    payload:,
    attempts:,
    created_at:,
    available_at:,
    reserved_at:,
    reserved_by:,
  ))
}

@internal
pub fn decode_succeeded_db_row() -> decode.Decoder(jobs.SucceededJob) {
  use id <- decode.field(0, decode.string)
  use name <- decode.field(1, decode.string)
  use payload <- decode.field(2, decode.string)
  use attempts <- decode.field(3, decode.int)
  use created_at <- decode.field(4, timestamp_decoder())
  use available_at <- decode.field(5, timestamp_decoder())
  use succeeded_at <- decode.field(6, timestamp_decoder())
  decode.success(jobs.SucceededJob(
    id:,
    name:,
    payload:,
    attempts:,
    created_at:,
    available_at:,
    succeeded_at:,
  ))
}

@internal
pub fn decode_failed_db_row() -> decode.Decoder(jobs.FailedJob) {
  use id <- decode.field(0, decode.string)
  use name <- decode.field(1, decode.string)
  use payload <- decode.field(2, decode.string)
  use attempts <- decode.field(3, decode.int)
  use exception <- decode.field(4, decode.string)
  use created_at <- decode.field(5, timestamp_decoder())
  use available_at <- decode.field(6, timestamp_decoder())
  use failed_at <- decode.field(7, timestamp_decoder())

  decode.success(jobs.FailedJob(
    id:,
    name:,
    payload:,
    attempts:,
    exception:,
    created_at:,
    available_at:,
    failed_at:,
  ))
}

@internal
pub fn to_db_date(date: tempo.NaiveDateTime) -> String {
  date
  |> naive_datetime.format("YYYY-MM-DD HH:mm:ss")
}

fn timestamp_decoder() {
  use str <- decode.then(decode.string)
  case naive_datetime.parse(str, "YYYY-MM-DD HH:mm:ss") {
    Ok(timestamp) -> decode.success(timestamp |> naive_datetime.to_tuple)
    Error(_) -> {
      decode.failure(#(#(0, 0, 0), #(0, 0, 0)), "timestamp")
    }
  }
}
