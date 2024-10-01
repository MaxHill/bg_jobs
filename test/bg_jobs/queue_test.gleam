import bg_jobs/job
import bg_jobs/queue
import bg_jobs/sqlite_store
import birdie
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/otp/supervisor
import gleam/result
import gleeunit/should
import pprint
import sqlight
import test_helpers
import test_helpers/jobs
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/test_logger

pub fn single_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queue, _queue_name, _job_store, logger)) = jobs.setup(conn)

  let assert Ok(Ok(_job)) =
    log_job.dispatch(queue, log_job.LogPayload("test message"))

  queue.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(15)

  test_logger.get_log(logger)
  |> pprint.format
  |> birdie.snap(title: "Job processed Ok and line logged")
}

pub fn job_is_moved_to_success_after_succeeding_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queue, _queue_name, job_store, _logger)) = jobs.setup(conn)

  let _ = log_job.dispatch(queue, log_job.LogPayload("test message1"))
  queue.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(15)

  job_store.get_succeeded_jobs(10)
  |> result.map(list.map(_, fn(job: job.SucceededJob) {
    #(job.queue, job.name, job.payload, job.attempts)
  }))
  |> pprint.format
  |> birdie.snap(title: "Job moved to success after succeeding")
}

pub fn process_muliptle_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queue, _queue_name, _job_store, logger)) = jobs.setup(conn)

  let _ = log_job.dispatch(queue, log_job.LogPayload("test message1"))
  let _ = log_job.dispatch(queue, log_job.LogPayload("test message2"))
  let _ = log_job.dispatch(queue, log_job.LogPayload("test message3"))

  queue.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(15)

  test_logger.get_log(logger)
  |> pprint.format
  |> birdie.snap(title: "Multiple jobs processed")
}

pub fn failing_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queue, _queue_name, job_store, logger)) = jobs.setup(conn)

  let assert Ok(Ok(_job)) =
    failing_job.dispatch(queue, failing_job.FailingPayload("Failing"))

  queue.process_jobs(queue)

  // Wait for jobs to process
  process.sleep(15)

  job_store.get_failed_jobs(1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(failed_job) {
    failed_job.attempts
    |> should.equal(3)
  }

  test_logger.get_log(logger)
  |> pprint.format
  |> birdie.snap(title: "Job process errored and line logged")
}

pub fn handle_no_worker_found_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queue, _queue_name, job_store, logger)) = jobs.setup(conn)

  // Dispatch non-existing job causing a panic
  let assert Ok(_) = queue.enqueue_job(queue, "non-existing", "test-payload")
  queue.process_jobs(queue)

  // Get newly restarted job
  let assert Ok(_) = log_job.dispatch(queue, log_job.LogPayload("testing"))

  queue.process_jobs(queue)
  process.sleep(10)

  test_logger.get_log(logger)
  |> should.equal(["testing"])

  job_store.get_failed_jobs(1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(job) {
    job.name
    |> should.equal("non-existing")

    job.exception
    |> should.equal("Could not find worker for job")
  }
}
