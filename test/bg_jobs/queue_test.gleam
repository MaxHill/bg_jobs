import bg_jobs/job
import bg_jobs/queue
import bg_jobs/sqlite_store
import birdie
import gleam/erlang/process
import gleam/list
import gleam/otp/supervisor
import gleam/result
import gleeunit/should
import pprint
import singularity
import sqlight
import test_helpers
import test_helpers/jobs
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/test_logger

pub fn single_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queues, _job_store, logger)) = jobs.setup(conn)

  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  let assert Ok(_job) = log_job.dispatch(queue, log_job.Payload("test message"))

  // Wait for jobs to process
  process.sleep(15)

  test_logger.get_log(logger)
  |> pprint.format
  |> birdie.snap(title: "Job processed Ok and line logged")
}

pub fn job_is_moved_to_success_after_succeeding_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queues, job_store, _logger)) = jobs.setup(conn)

  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  let _ = log_job.dispatch(queue, log_job.Payload("test message 1"))

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
  let assert Ok(#(queues, _job_store, logger)) = jobs.setup(conn)

  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  let _ = log_job.dispatch(queue, log_job.Payload("test message 1"))
  let _ = log_job.dispatch(queue, log_job.Payload("test message 2"))
  let _ = log_job.dispatch(queue, log_job.Payload("test message 3"))
  let _ = log_job.dispatch(queue, log_job.Payload("test message 4"))
  let _ = log_job.dispatch(queue, log_job.Payload("test message 5"))

  // Wait for jobs to process
  process.sleep(15)

  test_logger.get_log(logger)
  |> pprint.format
  |> birdie.snap(title: "Multiple jobs processed")
}

pub fn failing_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(#(queues, job_store, logger)) = jobs.setup(conn)
  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  let assert Ok(_job) =
    failing_job.dispatch(queue, failing_job.FailingPayload("Failing"))

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
  let assert Ok(#(queues, job_store, logger)) = jobs.setup(conn)
  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  let assert Ok(_) = queue.enqueue_job(queue, "non-existing", "test-payload")
  let assert Ok(_) =
    jobs.dispatch(queues, jobs.LogJob(log_job.Payload("testing")))
  // let assert Ok(_) = log_job.dispatch(queue, log_job.Payload("testing"))

  process.sleep(10)

  // Make sure one of the jobs worked
  test_logger.get_log(logger)
  |> should.equal(["testing"])

  // Make suer one of the jobs failed
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

pub fn handle_panic_test() {
  use conn <- sqlight.with_connection(":memory:")
  use _ <- result.map(test_helpers.reset_db(conn))
  let logger = test_logger.new_logger()

  let bad_store =
    queue.JobStore(
      ..sqlite_store.try_new_store(conn),
      get_next_jobs: fn(_, _) { panic as "test panic" },
    )

  let assert Ok(queues) = singularity.start()

  let default_queue =
    queue.new_otp_worker(
      registry: queues,
      queue_type: jobs.DefaultQueue,
      max_retries: 3,
      job_store: bad_store,
      workers: [log_job.worker(logger), failing_job.worker(logger)],
    )

  let assert Ok(_sup) =
    supervisor.start_spec(
      supervisor.Spec(
        argument: Nil,
        frequency_period: 1,
        max_frequency: 5,
        init: fn(children) {
          children
          |> supervisor.add(default_queue)
        },
      ),
    )

  let assert jobs.DefaultQueue(queue) =
    queue.get_queue(queues, jobs.DefaultQueue)
  let assert Ok(_job) = log_job.dispatch(queue, log_job.Payload("test message"))

  // Wait for restart
  process.sleep(10)

  let assert jobs.DefaultQueue(restarted_queue) =
    queue.get_queue(queues, jobs.DefaultQueue)

  restarted_queue
  |> should.not_equal(queue)

  let assert Ok(_job) =
    log_job.dispatch(restarted_queue, log_job.Payload("test message"))
}
