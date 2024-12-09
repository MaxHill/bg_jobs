import bg_jobs
import bg_jobs/db_adapter
import bg_jobs/internal/monitor
import bg_jobs/internal/monitor_messages
import bg_jobs/internal/utils
import chip
import gleam/erlang/process
import gleam/io
import gleam/list
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/forever_job

pub fn test_thing(
  bg: bg_jobs.BgJobs,
  db_adapter: db_adapter.DbAdapter,
  logger: process.Subject(test_helpers.LogMessage),
) {
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
  |> list.length()
  |> should.equal(2)
}

pub fn release_claimed_jobs_on_process_down_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, logger, _event_logger) <- jobs_setup.setup(conn)

  let assert Ok(_job) = forever_job.dispatch(bg)

  process.sleep(100)
  test_thing(bg, db_adapter, logger)
  test_thing(bg, db_adapter, logger)
}

pub fn release_claimed_jobs_on_process_down_interval_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, _logger, _event_logger) <- jobs_setup.setup_interval(
    conn,
  )
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

  // Kill the queue again, this should trigger the cleanup again
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

pub fn monitor_restart_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(_bg, _db_adapter, _logger, _event_logger) <- jobs_setup.setup(conn)

  // Wait for registration to happen
  process.sleep(100)

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

  // process.send(sub1, monitor_messages.Init)

  // wait for restart
  process.sleep(100)

  // Make sure it's a new monitor
  // monitor.get_monitor_subject()
  // |> should.be_some()
  // |> should.not_equal(sub1)

  monitor.get_table()
  |> should.be_ok()
  |> monitor.get_all_monitoring()
  |> list.map(fn(d) {
    case d.1 {
      monitor.MonitorQueue(pid, name, _, _)
      | monitor.MonitorScheduledJob(pid, name, _, _)
      | monitor.MonitorMonitor(pid, name, _) ->
        io.debug(#(name, process.is_alive(pid)))
    }
    d
  })
  |> list.map(fn(process) {
    io.debug(#("Is alive", process.is_alive({ process.1 }.pid)))
    process
  })
  |> list.length()
  |> should.equal(list.length(all_monitoring))
  Nil
  // get monitor
}
