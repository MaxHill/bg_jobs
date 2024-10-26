import bg_jobs
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import sqlight
import test_helpers
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job

// Default queue settings
pub fn queue(queue_name: String) {
  bg_jobs.new_queue(queue_name)
  |> bg_jobs.queue_with_poll_interval_ms(10)
  |> bg_jobs.queue_with_max_concurrent_jobs(4)
}

pub fn setup(
  conn: sqlight.Connection,
  f: fn(
    #(bg_jobs.BgJobs, bg_jobs.DbAdapter, process.Subject(_), process.Subject(_)),
  ) ->
    Nil,
) {
  let logger = test_helpers.new_logger()
  let event_logger = test_helpers.new_logger()
  let logger_event_listner = test_helpers.new_logger_event_listner(
    event_logger,
    _,
  )
  let db_adapter = sqlite_db_adapter.try_new_store(conn, [])

  let assert Ok(_) = db_adapter.migrate_down()
  let assert Ok(_) = db_adapter.migrate_up()

  let assert Ok(bg) =
    bg_jobs.new(db_adapter)
    |> bg_jobs.add_event_listener(logger_event_listner)
    |> bg_jobs.add_queue(
      queue("default_queue")
      |> bg_jobs.queue_with_workers([
        failing_job.worker(logger),
        log_job.worker(logger),
      ]),
    )
    |> bg_jobs.add_queue(queue("second_queue"))
    |> bg_jobs.create()

  f(#(bg, db_adapter, logger, event_logger))

  // Post test cleanup
  bg_jobs.stop_processing_all(bg)
  // Give it time to stop polling before connection closes
  process.sleep(100)
}
