import bg_jobs
import bg_jobs/sqlite_store
import chip
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/list
import gleam/result
import gleeunit
import gleeunit/should
import sqlight
import test_helpers/jobs
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/test_logger

pub fn main() {
  gleeunit.main()
}

pub fn single_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queue_store, _db_adapter, logger) = jobs.setup(conn)

  let assert Ok(_job) =
    log_job.dispatch(queue_store, log_job.Payload("test message"))

  // need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(100)

  test_logger.get_log(logger)
  |> should.equal(["test message"])
}

pub fn job_is_moved_to_success_after_succeeding_test() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queue_store, db_adapter, _logger) = jobs.setup(conn)

  let assert Ok(_job) =
    log_job.dispatch(queue_store, log_job.Payload("test message 1"))

  // Need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(100)

  db_adapter.get_succeeded_jobs(10)
  |> result.map(list.map(_, fn(job: bg_jobs.SucceededJob) {
    #(job.name, job.payload, job.attempts)
  }))
  |> should.be_ok
  |> should.equal([#("LOG_JOB", "\"test message 1\"", 0)])
}

pub fn process_muliptle_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queue_store, _db_adapter, logger) = jobs.setup(conn)
  let dispatch = log_job.dispatch(queue_store, _)

  let assert Ok(_) = dispatch(log_job.Payload("test message 1"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 2"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 3"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 4"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 5"))

  // Need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(100)

  test_logger.get_log(logger)
  |> should.equal([
    "test message 1", "test message 2", "test message 3", "test message 4",
    "test message 5",
  ])
}

pub fn failing_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queue_store, db_adapter, logger) = jobs.setup(conn)

  let assert Ok(_job) =
    failing_job.dispatch(queue_store, failing_job.FailingPayload("Failing"))

  // Need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(15)

  db_adapter.get_failed_jobs(1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(failed_job) {
    failed_job.attempts
    |> should.equal(3)
  }

  test_logger.get_log(logger)
  |> should.equal([
    "Attempt: 0 - Failed with payload: Failing",
    "Attempt: 1 - Failed with payload: Failing",
    "Attempt: 2 - Failed with payload: Failing",
    "Attempt: 3 - Failed with payload: Failing",
  ])
}

pub fn handle_no_worker_found_test() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queue_store, db_adapter, logger) = jobs.setup(conn)

  let assert Error(_) =
    bg_jobs.enqueue_job(
      queue_store,
      "DOES_NOT_EXIST",
      json.to_string(json.string("payload")),
    )

  // Need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)
  process.sleep(100)

  let assert Ok(_) = log_job.dispatch(queue_store, log_job.Payload("testing"))
  bg_jobs.process_jobs(queue)

  // Need to manually trigger this now
  let assert Ok(queue) = chip.find(queue_store, "default_queue")
  bg_jobs.process_jobs(queue)
  process.sleep(100)

  // Make sure one of the jobs worked
  test_logger.get_log(logger)
  |> should.equal(["testing"])
}

pub fn handle_panic_test() {
  use conn <- sqlight.with_connection(":memory:")
  let bad_adapter =
    bg_jobs.DbAdapter(
      ..sqlite_store.try_new_store(conn),
      get_next_jobs: fn(_, _) { panic as "test panic" },
    )
  let assert Ok(_) = bad_adapter.migrate_down()
  let assert Ok(_) = bad_adapter.migrate_up()
  let logger = test_logger.new_logger()

  let assert Ok(#(_sup, queues)) =
    bg_jobs.setup(
      bg_jobs.QueueSupervisorSpec(
        max_frequency: 1,
        frequency_period: 1,
        db_adapter: bad_adapter,
        queues: [
          bg_jobs.QueueSpec(
            name: "default_queue",
            workers: [log_job.worker(logger)],
            max_retries: 3,
            init_timeout: 1000,
          ),
        ],
      ),
    )
  let assert Ok(queue) = chip.find(queues, "default_queue")
  let assert Ok(_job) =
    log_job.dispatch(queues, log_job.Payload("test message"))

  // Wait for restart
  bg_jobs.process_jobs(queue)
  process.sleep(10)

  let assert Ok(restarted_queue) = chip.find(queues, "default_queue")

  restarted_queue
  |> should.not_equal(queue)
}
