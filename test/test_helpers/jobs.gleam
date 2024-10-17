import bg_jobs
import bg_jobs/sqlite_db_adapter
import chip
import gleam/erlang/process
import sqlight
import test_helpers
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job

/// Supervisor worker for a queue 
pub fn queue(queue_name: String, workers: List(bg_jobs.Worker)) {
  bg_jobs.QueueSpec(
    name: queue_name,
    max_retries: 3,
    workers: workers,
    init_timeout: 100,
    poll_interval: 10,
    max_concurrent_jobs: 1,
  )
}

pub fn setup(
  conn: sqlight.Connection,
  f: fn(
    #(
      process.Subject(chip.Message(bg_jobs.Message, String, Nil)),
      bg_jobs.DbAdapter,
      process.Subject(_),
      process.Subject(_),
    ),
  ) ->
    Nil,
) {
  let logger = test_helpers.new_logger()
  let event_logger = test_helpers.new_logger()
  let db_adapter = sqlite_db_adapter.try_new_store(conn, [])
  let assert Ok(_) = db_adapter.migrate_down()
  let assert Ok(_) = db_adapter.migrate_up()

  let assert Ok(#(_sup, registry)) =
    bg_jobs.setup(
      bg_jobs.QueueSupervisorSpec(
        max_frequency: 5,
        frequency_period: 1,
        event_listners: [test_helpers.new_logger_event_listner(event_logger, _)],
        db_adapter: db_adapter,
        queues: [
          queue("default_queue", [
            log_job.worker(logger),
            failing_job.worker(logger),
          ]),
          queue("second_queue", []),
        ],
      ),
    )
  let assert Ok(_default_queue) = chip.find(registry, "default_queue")

  f(#(registry, db_adapter, logger, event_logger))

  // Cleanup
  bg_jobs.stop_processing_all(registry)
  // Give it time to stop polling before connection closes
  process.sleep(100)
}
