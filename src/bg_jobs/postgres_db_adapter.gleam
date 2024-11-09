import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/internal/events
import bg_jobs/internal/postgres_db_adapter/sql
import bg_jobs/internal/utils
import bg_jobs/jobs
import birl
import decode
import gleam/dynamic
import gleam/int
import gleam/list
import gleam/pgo
import gleam/result
import gleam/string
import youid/uuid

/// Create a new postgres_db_adapter
/// 
pub fn new(conn: pgo.Connection, event_listners: List(events.EventListener)) {
  let send_event = events.send_event(event_listners, _)
  db_adapter.DbAdapter(
    enqueue_job: enqueue_job(conn, send_event),
    claim_jobs: claim_jobs(conn, send_event),
    release_claim: release_claim(conn, send_event),
    move_job_to_succeeded: move_job_to_succeeded(conn, send_event),
    move_job_to_failed: move_job_to_failed(conn, send_event),
    increment_attempts: increment_attempts(conn, send_event),
    get_enqueued_jobs: get_enqueued_jobs(conn, send_event),
    get_running_jobs: get_running_jobs(conn, send_event),
    get_succeeded_jobs: get_succeeded_jobs(conn, send_event),
    get_failed_jobs: get_failed_jobs(conn, send_event),
    migrate_up: migrate_up(conn),
    migrate_down: migrate_down(conn),
  )
}

fn move_job_to_succeeded(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job) {
    send_event(events.DbEvent("move_job_to_succeeded", [string.inspect(job)]))
    use _ <- result.try(
      sql.delete_enqueued_job(conn, job.id)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    sql.insert_succeeded_job(
      conn,
      job.id,
      job.name,
      job.payload,
      job.attempts,
      job.created_at,
      job.available_at,
      birl.now() |> birl.to_erlang_datetime(),
    )
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
    |> result.replace(Nil)
  }
}

fn move_job_to_failed(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job, exception: String) {
    send_event(
      events.DbEvent("move_job_to_failed", [string.inspect(job), exception]),
    )
    use _ <- result.try(
      sql.delete_enqueued_job(conn, job.id)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError)
      |> result.map_error(fn(err) {
        send_event(events.DbErrorEvent(err))
        err
      }),
    )

    sql.insert_failed_job(
      conn,
      job.id,
      job.name,
      job.payload,
      job.attempts,
      exception,
      job.created_at,
      job.available_at,
      birl.now() |> birl.to_erlang_datetime(),
    )
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
    |> result.replace(Nil)
  }
}

fn get_succeeded_jobs(conn: pgo.Connection, send_event: events.EventListener) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_succeeded_jobs", [int.to_string(limit)]))
    sql.get_succeeded_jobs(conn, limit)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned

      row
      |> list.map(fn(job_row) {
        jobs.SucceededJob(
          id: job_row.id,
          name: job_row.name,
          payload: job_row.payload,
          attempts: job_row.attempts,
          created_at: job_row.created_at,
          available_at: job_row.available_at,
          succeeded_at: job_row.succeeded_at,
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

fn get_failed_jobs(conn: pgo.Connection, send_event: events.EventListener) {
  fn(limit: Int) {
    send_event(events.DbEvent("get_failed_jobs", [int.to_string(limit)]))
    sql.get_failed_jobs(conn, limit)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned

      row
      |> list.map(fn(job_row) {
        jobs.FailedJob(
          id: job_row.id,
          name: job_row.name,
          payload: job_row.payload,
          attempts: job_row.attempts,
          exception: job_row.exception,
          created_at: job_row.created_at,
          available_at: job_row.available_at,
          failed_at: job_row.failed_at,
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

fn claim_jobs(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job_names: List(String), limit: Int, queue_id: String) {
    send_event(events.DbEvent("claim_jobs", [string.inspect(job_names)]))
    let now =
      birl.now()
      |> birl.to_erlang_datetime()

    let job_names_sql =
      list.length(job_names)
      |> list.range(1, _)
      |> list.map(fn(i) { "$" <> int.to_string(i + 4) })
      |> string.join(with: ",")

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
      )
      RETURNING *;
    "

    let arguments =
      list.flatten([
        [
          pgo.timestamp(now),
          pgo.text(queue_id),
          pgo.timestamp(now),
          pgo.int(limit),
        ],
        list.map(job_names, pgo.text),
      ])

    pgo.execute(sql, conn, arguments, decode_enqueued_db_row)
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
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, rows) = returned
      rows
    })
  }
}

fn release_claim(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job_id: String) {
    send_event(events.DbEvent("claim_jobs", [job_id]))
    sql.release_claim(conn, job_id)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned
      let assert Ok(row) = list.first(row)

      jobs.Job(
        id: row.id,
        name: row.name,
        payload: row.payload,
        attempts: row.attempts,
        created_at: row.created_at,
        available_at: row.available_at,
        reserved_at: row.reserved_at,
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

fn get_running_jobs(conn: pgo.Connection, send_event: events.EventListener) {
  fn(queue_id: String) {
    send_event(events.DbEvent("get_running_jobs", [queue_id]))
    sql.get_running_jobs(conn, queue_id)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned
      list.map(row, fn(job) {
        jobs.Job(
          id: job.id,
          name: job.name,
          payload: job.payload,
          attempts: job.attempts,
          created_at: job.created_at,
          available_at: job.available_at,
          reserved_at: job.reserved_at,
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

fn get_enqueued_jobs(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job_name: String) {
    send_event(events.DbEvent("get_enqueued_jobs", [job_name]))
    sql.get_enqueued_jobs(conn, job_name)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned
      list.map(row, fn(job) {
        jobs.Job(
          id: job.id,
          name: job.name,
          payload: job.payload,
          attempts: job.attempts,
          created_at: job.created_at,
          available_at: job.available_at,
          reserved_at: job.reserved_at,
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

fn increment_attempts(conn: pgo.Connection, send_event: events.EventListener) {
  fn(job: jobs.Job) {
    send_event(events.DbEvent("increment_attempts", [job.id, job.name]))
    sql.increment_attempts(conn, job.id)
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned
      let assert Ok(job) = list.first(row)
      jobs.Job(
        id: job.id,
        name: job.name,
        payload: job.payload,
        attempts: job.attempts,
        created_at: job.created_at,
        available_at: job.available_at,
        reserved_at: job.reserved_at,
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

fn enqueue_job(conn: pgo.Connection, send_event: events.EventListener) {
  fn(
    job_name: String,
    payload: String,
    avaliable_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  ) {
    send_event(
      events.DbEvent("enqueue_job", [
        job_name,
        payload,
        birl.from_erlang_universal_datetime(avaliable_at) |> birl.to_iso8601(),
      ]),
    )
    sql.enqueue_job(
      conn,
      uuid.v4_string(),
      job_name,
      payload,
      birl.now() |> birl.to_erlang_datetime(),
      avaliable_at,
    )
    |> result.map(fn(returned) {
      let pgo.Returned(_row_count, row) = returned
      let assert Ok(job) = list.first(row)
      jobs.Job(
        id: job.id,
        name: job.name,
        payload: job.payload,
        attempts: job.attempts,
        created_at: job.created_at,
        available_at: job.available_at,
        reserved_at: job.reserved_at,
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
pub fn migrate_up(conn: pgo.Connection) {
  fn() {
    use _ <- result.try(
      pgo.execute(
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
        conn,
        [],
        utils.discard_decode,
      )
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    use _ <- result.try(
      pgo.execute(
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
        conn,
        [],
        utils.discard_decode,
      )
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    use _ <- result.try(
      pgo.execute(
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
        conn,
        [],
        utils.discard_decode,
      )
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )

    Ok(Nil)
  }
}

@internal
pub fn migrate_down(conn: pgo.Connection) {
  fn() {
    use _ <- result.try(
      pgo.execute("DROP TABLE IF EXISTS jobs;", conn, [], utils.discard_decode)
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )
    use _ <- result.try(
      pgo.execute(
        "DROP TABLE IF EXISTS jobs_failed;",
        conn,
        [],
        utils.discard_decode,
      )
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )
    use _ <- result.try(
      pgo.execute(
        "DROP TABLE IF EXISTS jobs_succeeded;",
        conn,
        [],
        utils.discard_decode,
      )
      |> result.map_error(string.inspect)
      |> result.map_error(errors.DbError),
    )
    Ok(Nil)
  }
}

@internal
pub fn decode_enqueued_db_row(data: dynamic.Dynamic) {
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
      jobs.Job(
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

  decoder
  |> decode.from(data)
}

@internal
pub fn decode_succeeded_db_row(data: dynamic.Dynamic) {
  let decoder =
    decode.into({
      use id <- decode.parameter
      use name <- decode.parameter
      use payload <- decode.parameter
      use attempts <- decode.parameter
      use created_at <- decode.parameter
      use available_at <- decode.parameter
      use succeeded_at <- decode.parameter
      jobs.SucceededJob(
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

  decoder
  |> decode.from(data)
}

@internal
pub fn decode_failed_db_row(data: dynamic.Dynamic) {
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
      jobs.FailedJob(
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

  decoder
  |> decode.from(data)
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
