import bg_jobs/internal/time
import bg_jobs/scheduled_job
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import gleam/list
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/log_job_interval

pub fn single_interval_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(_bg, db_adapter, logger, _event_logger) <- jobs_setup.setup_interval(
    conn,
  )

  // Wait for jobs to process
  process.sleep(100)

  // Make sure the scheduled job succeded
  test_helpers.get_log(logger)
  |> list.find(fn(log) { log == "test_log" })
  |> should.equal(Ok("test_log"))

  let _ =
    db_adapter.get_succeeded_jobs(1)
    |> should.be_ok()
    |> list.first()
    |> should.be_ok()

  test_helpers.get_log(logger)
  |> list.find(fn(log) { log == "Attempt: 2" })
  |> should.equal(Ok("Attempt: 2"))

  let _ =
    db_adapter.get_failed_jobs(1)
    |> should.be_ok()
    |> list.first()
    |> should.be_ok()

  Nil
}

pub fn schedule_test() {
  use conn <- sqlight.with_connection(":memory:")
  let logger = test_helpers.new_logger()
  use #(_bg, _db_adapter, _event_logger) <- jobs_setup.setup_schedule(
    conn,
    scheduled_job.Spec(
      schedule: scheduled_job.Interval(time.Millisecond(10)),
      worker: log_job_interval.worker(logger),
      max_retries: 3,
      init_timeout: 1000,
      poll_interval: 10,
      event_listeners: [],
    ),
  )

  process.sleep(100)

  let job_list =
    sqlight.query(
      "SELECT * FROM JOBS",
      conn,
      [],
      sqlite_db_adapter.decode_enqueued_db_row,
    )
    |> should.be_ok
    |> list.map(should.be_ok)

  job_list
  |> list.length
  |> should.equal(1)

  job_list
  |> list.first
  |> should.be_ok
  |> fn(job) { job.name |> should.equal("LOG_JOB_SCHEDULE") }
}
