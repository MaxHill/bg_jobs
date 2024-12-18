import bg_jobs
import bg_jobs/db_adapter
import bg_jobs/queue
import bg_jobs/scheduled_job
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import gleam/otp/static_supervisor as sup
import sqlight
import test_helpers
import test_helpers/jobs/failing_job
import test_helpers/jobs/failing_job_interval
import test_helpers/jobs/forever_job
import test_helpers/jobs/forever_job_interval
import test_helpers/jobs/log_job
import test_helpers/jobs/log_job_interval

// Default queue settings
pub fn queue(queue_name: String) {
  queue.new(queue_name)
  |> queue.with_poll_interval_ms(10)
  |> queue.with_max_concurrent_jobs(4)
}

pub fn setup(
  conn: sqlight.Connection,
  f: fn(
    #(
      bg_jobs.BgJobs,
      db_adapter.DbAdapter,
      process.Subject(_),
      process.Subject(_),
    ),
  ) ->
    Nil,
) {
  let logger = test_helpers.new_logger()
  let event_logger = test_helpers.new_logger()
  let logger_event_listner = test_helpers.new_logger_event_listner(
    event_logger,
    _,
  )
  let db_adapter = sqlite_db_adapter.new(conn, [])

  let assert Ok(_) = db_adapter.migrate_down([])
  let assert Ok(_) = db_adapter.migrate_up([])

  let assert Ok(bg) =
    sup.new(sup.OneForOne)
    // Max 100 restarts in 1 second
    |> sup.restart_tolerance(100, 1)
    |> bg_jobs.new(db_adapter)
    |> bg_jobs.with_event_listener(logger_event_listner)
    |> bg_jobs.with_queue(
      queue("default_queue")
      |> queue.with_workers([
        failing_job.worker(logger),
        log_job.worker(logger),
        forever_job.worker(logger),
      ]),
    )
    |> bg_jobs.with_queue(queue("second_queue"))
    |> bg_jobs.build()

  f(#(bg, db_adapter, logger, event_logger))

  // Post test cleanup
  test_helpers.cleanup_processes(bg)
}

pub fn setup_interval(
  conn: sqlight.Connection,
  f: fn(
    #(
      bg_jobs.BgJobs,
      db_adapter.DbAdapter,
      process.Subject(_),
      process.Subject(_),
    ),
  ) ->
    Nil,
) {
  let logger = test_helpers.new_logger()
  let event_logger = test_helpers.new_logger()
  let logger_event_listner = test_helpers.new_logger_event_listner(
    event_logger,
    _,
  )
  let db_adapter = sqlite_db_adapter.new(conn, [])

  let assert Ok(_) = db_adapter.migrate_down([])
  let assert Ok(_) = db_adapter.migrate_up([])

  let assert Ok(bg) =
    sup.new(sup.OneForOne)
    |> bg_jobs.new(db_adapter)
    |> bg_jobs.with_event_listener(logger_event_listner)
    |> bg_jobs.with_scheduled_job(
      scheduled_job.new(
        worker: log_job_interval.worker(logger),
        schedule: scheduled_job.new_interval_milliseconds(10),
      )
      |> scheduled_job.with_poll_interval_ms(10),
    )
    |> bg_jobs.with_scheduled_job(
      scheduled_job.new(
        worker: failing_job_interval.worker(logger),
        schedule: scheduled_job.new_interval_milliseconds(10),
      )
      |> scheduled_job.with_poll_interval_ms(10),
    )
    |> bg_jobs.with_scheduled_job(
      scheduled_job.new(
        schedule: scheduled_job.new_interval_milliseconds(10),
        worker: forever_job_interval.worker(logger),
      )
      |> scheduled_job.with_poll_interval_ms(10),
    )
    |> bg_jobs.build()

  f(#(bg, db_adapter, logger, event_logger))

  // Post test cleanup
  test_helpers.cleanup_processes(bg)
}

pub fn setup_schedule(
  conn: sqlight.Connection,
  spec: scheduled_job.SpecBuilder,
  f: fn(#(bg_jobs.BgJobs, db_adapter.DbAdapter, process.Subject(_))) -> Nil,
) {
  let event_logger = test_helpers.new_logger()
  let logger_event_listner = test_helpers.new_logger_event_listner(
    event_logger,
    _,
  )
  let db_adapter = sqlite_db_adapter.new(conn, [])

  let assert Ok(_) = db_adapter.migrate_down([])
  let assert Ok(_) = db_adapter.migrate_up([])

  let assert Ok(bg) =
    sup.new(sup.OneForOne)
    |> bg_jobs.new(db_adapter)
    |> bg_jobs.with_event_listener(logger_event_listner)
    |> bg_jobs.with_scheduled_job(spec)
    |> bg_jobs.build()

  f(#(bg, db_adapter, event_logger))

  // Post test cleanup
  test_helpers.cleanup_processes(bg)
}
