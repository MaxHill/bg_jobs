import bg_jobs/internal/utils
import chip
import gleam/erlang/process
import gleam/list
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/forever_job

pub fn release_claimed_jobs_on_process_down_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, logger, _event_logger) <- jobs_setup.setup(conn)

  let assert Ok(_job) = forever_job.dispatch(bg)

  // Wait for jobs to process
  process.sleep(200)

  // Get queue
  let assert Ok(default_queue) = chip.find(bg.queue_registry, "default_queue")
  let queue_pid = process.subject_owner(default_queue)

  // Assert the job has been claimed
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(queue_pid))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.equal(utils.pid_to_string(queue_pid))
  })

  // Kill the queue, this should trigger the cleanup
  process.kill(queue_pid)

  //  Wait for restart
  process.sleep(50)

  // Assert the job has been claimed by the new actor
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(queue_pid))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.not_equal(utils.pid_to_string(queue_pid))
  })

  test_helpers.get_log(logger)
  // Job should have been started twice
  |> should.equal(["Running forever...", "Running forever..."])
}

pub fn release_claimed_jobs_on_process_down_interval_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, _logger, _event_logger) <- jobs_setup.setup_interval(
    conn,
  )

  // Give time for job to be started
  process.sleep(100)

  // Get scheduled job
  let assert Ok(scheduled_job) =
    chip.find(bg.scheduled_jobs_registry, "FOREVER_JOB_SCHEDULE")
  let scheduled_job_pid = process.subject_owner(scheduled_job)

  // Assert the job has been claimed
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    scheduled_job_pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.equal(utils.pid_to_string(scheduled_job_pid))
  })

  // Kill the queue, this should trigger the cleanup
  process.kill(scheduled_job_pid)

  //  Wait for restart
  process.sleep(50)

  // Assert the job has been claimed by the new actor
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    scheduled_job_pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.not_equal(utils.pid_to_string(scheduled_job_pid))
  })

  Nil
}
