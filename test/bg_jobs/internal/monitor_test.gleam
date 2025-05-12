import bg_jobs/db_adapter
import bg_jobs/internal/monitor
import bg_jobs/internal/queue_messages
import bg_jobs/internal/scheduled_jobs_messages
import bg_jobs/internal/utils
import gleam/erlang/process
import gleam/list
import gleam/option
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/forever_job

pub fn release_claimed_jobs_on_process_down_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, logger, _event_logger) <- jobs_setup.setup(conn)

  let assert Ok(_job) = forever_job.dispatch(bg)
  // Wait for job to be picked up
  process.sleep(10)

  // Test restart 4 times
  test_release_claim(1, db_adapter, logger)
  test_release_claim(2, db_adapter, logger)
  test_release_claim(3, db_adapter, logger)
  test_release_claim(4, db_adapter, logger)
}

fn test_release_claim(
  iteration: Int,
  db_adapter: db_adapter.DbAdapter,
  logger: process.Subject(test_helpers.LogMessage),
) {
  // Get queue
  let assert Ok(table) = monitor.get_table()
  let assert option.Some(default_queue) =
    monitor.get_by_name(table, "default_queue")

  // Assert the job has been claimed
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    default_queue.pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.equal(utils.pid_to_string(default_queue.pid))
  })

  // Kill the queue, this should trigger the cleanup

  let assert monitor.MonitorQueue(_, _, subject, _) = default_queue
  process.send(subject, queue_messages.Shutdown)
  process.kill(default_queue.pid)

  //  Wait for restart
  process.sleep(50)

  // Assert the job has been claimed by the new actor
  let new_default_queue =
    monitor.get_by_name(table, "default_queue")
    |> should.be_some()

  new_default_queue |> should.not_equal(default_queue)

  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    new_default_queue.pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.not_equal(utils.pid_to_string(default_queue.pid))
  })

  test_helpers.get_log(logger)
  |> list.length()
  |> should.equal(iteration + 1)
  Nil
}

pub fn release_claimed_jobs_on_process_down_interval_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(_bg, db_adapter, _logger, _event_logger) <- jobs_setup.setup_interval(
    conn,
  )
  process.sleep(10)
  // Get scheduled job
  let assert Ok(table) = monitor.get_table()
  let assert option.Some(scheduled_job) =
    monitor.get_by_name(table, "FOREVER_JOB_SCHEDULE")

  // Assert the job has been claimed
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    scheduled_job.pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.equal(utils.pid_to_string(scheduled_job.pid))
  })

  // Kill the queue, this should trigger the cleanup
  let assert monitor.MonitorScheduledJob(_, _, subject, _) = scheduled_job
  process.send(subject, scheduled_jobs_messages.Shutdown)
  process.kill(scheduled_job.pid)

  //  Wait for restart
  process.sleep(10)

  // Assert the job has been claimed by the new actor
  let new_scheduled_job =
    monitor.get_by_name(table, "FOREVER_JOB_SCHEDULE")
    |> should.be_some()
  new_scheduled_job
  |> should.not_equal(scheduled_job)
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    new_scheduled_job.pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.not_equal(utils.pid_to_string(scheduled_job.pid))
  })

  // Kill the queue again, this should trigger the cleanup again
  let monitor.MonitorScheduledJob(_, _, subject, _) = scheduled_job
  process.send(subject, scheduled_jobs_messages.Shutdown)
  process.kill(scheduled_job.pid)

  //  Wait for restart
  process.sleep(50)

  // Assert the job has been claimed by the new actor
  let new_scheduled_job =
    monitor.get_by_name(table, "FOREVER_JOB_SCHEDULE")
    |> should.be_some()
  new_scheduled_job
  |> should.not_equal(scheduled_job)
  db_adapter.get_running_jobs_by_queue_name(utils.pid_to_string(
    new_scheduled_job.pid,
  ))
  |> should.be_ok()
  |> list.map(fn(job) {
    job.reserved_by
    |> should.be_some()
    |> should.not_equal(utils.pid_to_string(scheduled_job.pid))
  })

  Nil
}

pub fn monitor_restart_bookkeeping_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(_bg, _db_adapter, _logger, _event_logger) <- jobs_setup.setup(conn)
  // Wait for register
  process.sleep(10)

  let all_monitoring =
    monitor.get_table()
    |> should.be_ok()
    |> monitor.get_all_monitoring()
    |> list.map(fn(pro) {
      let pid = { pro.1 }.pid
      should.be_true(process.is_alive(pid))
      pro
    })

  // Kill monitor
  let sub1 =
    monitor.get_monitor_subject()
    |> should.be_some()

  sub1
  |> process.subject_owner()
  |> process.kill

  // wait for restart
  process.sleep(10)

  // Make sure it's a new monitor
  monitor.get_monitor_subject()
  |> should.be_some()
  |> should.not_equal(sub1)

  // Make sure the new monitor is monitoring the same as the old one
  monitor.get_table()
  |> should.be_ok()
  |> monitor.get_all_monitoring()
  |> list.length()
  |> should.equal(list.length(all_monitoring))
  Nil
}
