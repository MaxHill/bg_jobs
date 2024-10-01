import bg_jobs
import bg_jobs/sqlite_store
import chip
import gleam/otp/supervisor
import sqlight
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/test_logger

/// Supervisor worker for a queue 
pub fn queue(queue_name: String, workers: List(bg_jobs.Worker)) {
  bg_jobs.QueueSpec(
    name: queue_name,
    max_retries: 3,
    workers: workers,
    init_timeout: 100,
  )
}

pub fn setup(conn: sqlight.Connection) {
  let db_adapter = sqlite_store.try_new_store(conn)
  let assert Ok(_) = db_adapter.migrate_down()
  let assert Ok(_) = db_adapter.migrate_up()
  let logger = test_logger.new_logger()

  let assert Ok(#(_sup, registry)) =
    bg_jobs.setup(
      bg_jobs.QueueSupervisorSpec(
        max_frequency: 5,
        frequency_period: 1,
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

  #(registry, db_adapter, logger)
}
