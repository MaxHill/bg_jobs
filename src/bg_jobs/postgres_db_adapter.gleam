import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/postgres_db_adapter/sql
import bg_jobs/internal/utils
import bg_jobs/jobs
import decode/zero
import gleam/dynamic
import gleam/int
import gleam/list
import gleam/option
import gleam/result
import gleam/string
import pog
import tempo/naive_datetime
import youid/uuid

/// Create a new postgres_db_adapter
/// 
pub fn new(conn: pog.Connection, event_listners: List(events.EventListener)) {
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

fn erlang_to_timestamp(timestamp: #(#(Int, Int, Int), #(Int, Int, Int))) {
  let #(#(year, month, day), #(hour, minute, seconds)) = timestamp
  pog.Timestamp(pog.Date(year, month, day), pog.Time(hour, minute, seconds, 0))
}

fn timestamp_to_erlang(timestamp: pog.Timestamp) {
  let pog.Timestamp(
    pog.Date(year, month, day),
    pog.Time(hour, minute, seconds, _milliesecond),
  ) = timestamp

  #(#(year, month, day), #(hour, minute, seconds))
}

/// If a job succeeds within the retries, this function 
/// will be called and move the job to the succeeded jobs table
/// 
fn move_job_to_succeeded(conn: pog.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job) {
    pog.transaction(conn, fn(conn) {
      send_event(events.DbEvent("move_job_to_succeeded", [string.inspect(job)]))
      use _ <- result.try(
        sql.delete_enqueued_job(conn, job.id)
        |> result.map_error(string.inspect),
        // |> result.map_error(errors.DbError),
      )

      sql.insert_succeeded_job(
        conn,
        job.id,
        job.name,
        job.payload,
        job.attempts,
        erlang_to_timestamp(job.created_at),
        erlang_to_timestamp(job.available_at),
        erlang_to_timestamp(
          naive_datetime.now_utc()
          |> naive_datetime.to_tuple(),
        ),
      )
      |> result.map(fn(res) {
        send_event(events.DbResponseEvent(string.inspect(res)))
        res
      })
      |> result.map_error(string.inspect)
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
    |> result.replace(Nil)
  }
}

/// If a job fails and cannot complete within the retries this function 
/// will be called and move the job to the faild jobs table
/// 
fn move_job_to_failed(conn: pog.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job, exception: String) {
    pog.transaction(conn, fn(conn) {
      send_event(
        events.DbEvent("move_job_to_failed", [string.inspect(job), exception]),
      )
      use _ <- result.try(
        sql.delete_enqueued_job(conn, job.id)
        |> result.map_error(string.inspect),
      )

      sql.insert_failed_job(
        conn,
        job.id,
        job.name,
        job.payload,
        job.attempts,
        exception,
        erlang_to_timestamp(job.created_at),
        erlang_to_timestamp(job.available_at),
        erlang_to_timestamp(
          naive_datetime.now_utc() |> naive_datetime.to_tuple(),
        ),
      )
      |> result.map(fn(res) {
        send_event(events.DbResponseEvent(string.inspect(res)))
        res
      })
      |> result.map_error(string.inspect)
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
    |> result.replace(Nil)
  }
}

/// Get all succeeded jobs
///
fn get_succeeded_jobs(conn: pog.Connection, send_event: events.EventListener) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_succeeded_jobs", [int.to_string(limit)]))
    sql.get_succeeded_jobs(conn, limit)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned

      row
      |> list.map(fn(job_row) {
        jobs.SucceededJob(
          id: job_row.id,
          name: job_row.name,
          payload: job_row.payload,
          attempts: job_row.attempts,
          created_at: timestamp_to_erlang(job_row.created_at),
          available_at: timestamp_to_erlang(job_row.available_at),
          succeeded_at: timestamp_to_erlang(job_row.succeeded_at),
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect(_))
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Get all failed jobs.
/// 
fn get_failed_jobs(conn: pog.Connection, send_event: events.EventListener) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_failed_jobs", [int.to_string(limit)]))
    sql.get_failed_jobs(conn, limit)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned

      row
      |> list.map(fn(job_row) {
        jobs.FailedJob(
          id: job_row.id,
          name: job_row.name,
          payload: job_row.payload,
          attempts: job_row.attempts,
          exception: job_row.exception,
          created_at: timestamp_to_erlang(job_row.created_at),
          available_at: timestamp_to_erlang(job_row.available_at),
          failed_at: timestamp_to_erlang(job_row.failed_at),
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect(_))
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Sets claimed_at and claimed_by and return the job to be processed.
/// 
fn reserve_jobs(conn: pog.Connection, send_event: events.EventListener) {
  fn(job_names: List(String), limit: Int, queue_id: String) {
    send_event(events.DbEvent("claim_jobs", [string.inspect(job_names)]))
    let now =
      naive_datetime.now_utc()
      |> naive_datetime.to_tuple()
      |> erlang_to_timestamp

    let job_names_sql =
      list.length(job_names)
      |> list.range(1, _)
      |> list.map(fn(i) { "$" <> int.to_string(i + 4) })
      |> string.join(with: ",")

    // This is inlined because it needs to be dynamic
    // over the different job_names we're interested in
    let sql = "
        UPDATE jobs
           SET reserved_at = $1, reserved_by = $2
         WHERE id IN (
          SELECT id
            FROM jobs
           WHERE name IN (" <> job_names_sql <> ")
             AND available_at <= $3
             AND reserved_at IS NULL  
        ORDER BY available_at ASC  
           LIMIT $4
           FOR UPDATE SKIP LOCKED
      )
      RETURNING *;
    "

    let arguments =
      list.flatten([
        [
          pog.timestamp(now),
          pog.text(queue_id),
          pog.timestamp(now),
          pog.int(limit),
        ],
        list.map(job_names, pog.text),
      ])

    pog.query(sql)
    |> pog.returning(decode_enqueued_db_row)
    |> list.fold(arguments, _, pog.parameter)
    |> pog.execute(conn)
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, rows) = returned
      rows
    })
  }
}

/// Release claim from job to allow another queue to process it
///
fn release_reservation(conn: pog.Connection, send_event: events.EventListener) {
  fn(job_id: String) {
    send_event(events.DbEvent("claim_jobs", [job_id]))
    sql.release_reservation(conn, job_id)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      let assert Ok(row) = list.first(row)

      jobs.Job(
        id: row.id,
        name: row.name,
        payload: row.payload,
        attempts: row.attempts,
        created_at: timestamp_to_erlang(row.created_at),
        available_at: timestamp_to_erlang(row.available_at),
        reserved_at: row.reserved_at |> option.map(timestamp_to_erlang),
        reserved_by: row.reserved_by,
      )
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Release claim from job that are clamied by a queue, to allow another queue to process it
///
fn release_jobs_reserved_by(
  conn: pog.Connection,
  send_event: events.EventListener,
) {
  fn(reserved_by: String) {
    send_event(events.DbEvent("claim_jobs", [reserved_by]))
    sql.release_jobs_reserved_by(conn, reserved_by)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, rows) = returned
      rows
      |> list.map(fn(row) {
        jobs.Job(
          id: row.id,
          name: row.name,
          payload: row.payload,
          attempts: row.attempts,
          created_at: timestamp_to_erlang(row.created_at),
          available_at: timestamp_to_erlang(row.available_at),
          reserved_at: row.reserved_at |> option.map(timestamp_to_erlang),
          reserved_by: row.reserved_by,
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Get a specific queues running jobs
///
fn get_running_jobs_by_queue_name(
  conn: pog.Connection,
  send_event: events.EventListener,
) {
  fn(queue_id: String) {
    send_event(events.DbEvent("get_running_jobs_by_queue_name", [queue_id]))
    sql.get_running_jobs_by_queue_name(conn, queue_id)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      list.map(row, fn(job) {
        jobs.Job(
          id: job.id,
          name: job.name,
          payload: job.payload,
          attempts: job.attempts,
          created_at: timestamp_to_erlang(job.created_at),
          available_at: timestamp_to_erlang(job.available_at),
          reserved_at: job.reserved_at |> option.map(timestamp_to_erlang),
          reserved_by: job.reserved_by,
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Get jobs from the database that has not been picked up by any queue
///
fn get_enqueued_jobs(conn: pog.Connection, send_event: events.EventListener) {
  fn(job_name: String) {
    send_event(events.DbEvent("get_enqueued_jobs", [job_name]))
    sql.get_enqueued_jobs(conn, job_name)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      list.map(row, fn(job) {
        jobs.Job(
          id: job.id,
          name: job.name,
          payload: job.payload,
          attempts: job.attempts,
          created_at: timestamp_to_erlang(job.created_at),
          available_at: timestamp_to_erlang(job.available_at),
          reserved_at: job.reserved_at |> option.map(timestamp_to_erlang),
          reserved_by: job.reserved_by,
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Get jobs from the database that has been picked up by any queue
///
fn get_running_jobs(conn: pog.Connection, send_event: events.EventListener) {
  fn() {
    send_event(events.DbEvent("get_running_jobs", []))
    sql.get_running_jobs(conn)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      list.map(row, fn(job) {
        jobs.Job(
          id: job.id,
          name: job.name,
          payload: job.payload,
          attempts: job.attempts,
          created_at: timestamp_to_erlang(job.created_at),
          available_at: timestamp_to_erlang(job.available_at),
          reserved_at: job.reserved_at |> option.map(timestamp_to_erlang),
          reserved_by: job.reserved_by,
        )
      })
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// If a job processing attempt has failed this should be called to increment the attempts
///
fn increment_attempts(conn: pog.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job) {
    send_event(events.DbEvent("increment_attempts", [job.id, job.name]))
    sql.increment_attempts(conn, job.id)
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      let assert Ok(job) = list.first(row)
      jobs.Job(
        id: job.id,
        name: job.name,
        payload: job.payload,
        attempts: job.attempts,
        created_at: timestamp_to_erlang(job.created_at),
        available_at: timestamp_to_erlang(job.available_at),
        reserved_at: job.reserved_at |> option.map(timestamp_to_erlang),
        reserved_by: job.reserved_by,
      )
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

/// Enqueues a job for queues to claim an process
///
fn enqueue_job(
  conn: pog.Connection,
  send_event: events.EventListener,
) -> fn(String, String, #(#(Int, Int, Int), #(Int, Int, Int))) ->
  Result(jobs.Job, errors.BgJobError) {
  fn(
    job_name: String,
    payload: String,
    avaliable_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  ) -> Result(jobs.Job, errors.BgJobError) {
    use avaliable_at_string <- result.try(
      utils.from_tuple(avaliable_at) |> result.map(naive_datetime.to_string),
    )
    send_event(
      events.DbEvent("enqueue_job", [job_name, payload, avaliable_at_string]),
    )
    sql.enqueue_job(
      conn,
      uuid.v4_string(),
      job_name,
      payload,
      naive_datetime.now_utc()
        |> naive_datetime.to_tuple()
        |> erlang_to_timestamp(),
      avaliable_at |> erlang_to_timestamp(),
    )
    |> result.map(fn(returned) {
      let pog.Returned(_row_count, row) = returned
      let assert Ok(job) = list.first(row)
      jobs.Job(
        id: job.id,
        name: job.name,
        payload: job.payload,
        attempts: job.attempts,
        created_at: timestamp_to_erlang(job.created_at),
        available_at: timestamp_to_erlang(job.available_at),
        reserved_at: job.reserved_at |> option.map(timestamp_to_erlang),
        reserved_by: job.reserved_by,
      )
    })
    |> result.map(fn(res) {
      send_event(events.DbResponseEvent(string.inspect(res)))
      res
    })
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)
    |> result.map_error(fn(err) {
      send_event(events.DbErrorEvent(err))
      err
    })
  }
}

@internal
pub fn migrate_up(conn: pog.Connection) {
  fn(event_listeners: List(events.EventListener)) {
    use _ <- result.try(
      pog.query(
        "
    CREATE TABLE IF NOT EXISTS jobs (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
      available_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
      reserved_at TIMESTAMP,
      reserved_by TEXT
    );",
      )
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    use _ <- result.try(
      pog.query(
        "
    CREATE TABLE IF NOT EXISTS jobs_failed (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      exception VARCHAR NOT NULL,
      created_at TIMESTAMP NOT NULL,
      available_at TIMESTAMP NOT NULL,
      failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
",
      )
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    use _ <- result.try(
      pog.query(
        "
    CREATE TABLE IF NOT EXISTS jobs_succeeded (
      id VARCHAR PRIMARY KEY NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      created_at TIMESTAMP NOT NULL,
      available_at TIMESTAMP NOT NULL,
      succeeded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
",
      )
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    events.send_event(event_listeners, events.MigrateUpComplete)
    Ok(Nil)
  }
}

@internal
pub fn migrate_down(conn: pog.Connection) {
  fn(event_listeners: List(events.EventListener)) {
    use _ <- result.try(
      pog.query("DROP TABLE IF EXISTS jobs;")
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )
    use _ <- result.try(
      pog.query("DROP TABLE IF EXISTS jobs_failed;")
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )
    use _ <- result.try(
      pog.query("DROP TABLE IF EXISTS jobs_succeeded;")
      |> pog.execute(conn)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    events.send_event(event_listeners, events.MigrateDownComplete)
    Ok(Nil)
  }
}

@internal
pub fn decode_enqueued_db_row(data: dynamic.Dynamic) {
  let decoder = {
    use id <- zero.field(0, zero.string)
    use name <- zero.field(1, zero.string)
    use payload <- zero.field(2, zero.string)
    use attempts <- zero.field(3, zero.int)
    use created_at <- zero.field(4, timestamp_decoder())
    use available_at <- zero.field(5, timestamp_decoder())
    use reserved_at <- zero.field(6, zero.optional(timestamp_decoder()))
    use reserved_by <- zero.field(7, zero.optional(zero.string))
    zero.success(jobs.Job(
      id:,
      name:,
      payload:,
      attempts:,
      created_at: timestamp_to_erlang(created_at),
      available_at: timestamp_to_erlang(available_at),
      reserved_at: reserved_at |> option.map(timestamp_to_erlang),
      reserved_by:,
    ))
  }

  zero.run(data, decoder)
}

@internal
pub fn decode_succeeded_db_row(data: dynamic.Dynamic) {
  let decoder = {
    use id <- zero.field(0, zero.string)
    use name <- zero.field(1, zero.string)
    use payload <- zero.field(2, zero.string)
    use attempts <- zero.field(3, zero.int)
    use created_at <- zero.field(4, timestamp_decoder())
    use available_at <- zero.field(5, timestamp_decoder())
    use succeeded_at <- zero.field(6, timestamp_decoder())
    zero.success(jobs.SucceededJob(
      id:,
      name:,
      payload:,
      attempts:,
      created_at: timestamp_to_erlang(created_at),
      available_at: timestamp_to_erlang(available_at),
      succeeded_at: timestamp_to_erlang(succeeded_at),
    ))
  }

  zero.run(data, decoder)
}

@internal
pub fn decode_failed_db_row(data: dynamic.Dynamic) {
  let decoder = {
    use id <- zero.field(0, zero.string)
    use name <- zero.field(1, zero.string)
    use payload <- zero.field(2, zero.string)
    use attempts <- zero.field(3, zero.int)
    use exception <- zero.field(4, zero.string)
    use created_at <- zero.field(5, timestamp_decoder())
    use available_at <- zero.field(6, timestamp_decoder())
    use failed_at <- zero.field(7, timestamp_decoder())
    zero.success(jobs.FailedJob(
      id:,
      name:,
      payload:,
      attempts:,
      exception:,
      created_at: timestamp_to_erlang(created_at),
      available_at: timestamp_to_erlang(available_at),
      failed_at: timestamp_to_erlang(failed_at),
    ))
  }

  zero.run(data, decoder)
}

// --- UTILS -------------------------------------------------------------------

/// A decoder to decode `timestamp`s coming from a Postgres query.
///
fn timestamp_decoder() {
  use dynamic <- zero.then(zero.dynamic)
  case pog.decode_timestamp(dynamic) {
    Ok(timestamp) -> zero.success(timestamp)
    Error(_) -> {
      let date = pog.Date(0, 0, 0)
      let time = pog.Time(0, 0, 0, 0)
      zero.failure(pog.Timestamp(date:, time:), "timestamp")
    }
  }
}
