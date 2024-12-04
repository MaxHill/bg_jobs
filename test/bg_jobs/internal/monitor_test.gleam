import bg_jobs/internal/monitor
import bg_jobs/internal/monitor_messages
import bg_jobs/internal/utils
import bg_jobs/sqlite_db_adapter
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/otp/actor
import gleam/otp/static_supervisor as sup
import gleam/result
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/forever_job

// pub fn monitor_demonitior_test() {
//   use conn <- sqlight.with_connection(":memory:")

//   // Database
//   let db_adapter = sqlite_db_adapter.new(conn, [])
//   let assert Ok(_) = db_adapter.migrate_down([])
//   let assert Ok(_) = db_adapter.migrate_up([])
//
//   let assert Ok(registry) = chip.start(chip.Unnamed)
//
//   let assert Ok(monitor) = monitor.build(registry, db_adapter)
//
//   // Supervision tree with 2 children that are registered with monitor
//   let parent_subject = process.new_subject()
//   let child_worker =
//     sup.worker_child("child1", fn() {
//       new_nil_actor(parent_subject, monitor)
//       |> result.map(process.subject_owner)
//     })
//   let parent_subject2 = process.new_subject()
//   let child_worker2 =
//     sup.worker_child("child2", fn() {
//       new_nil_actor(parent_subject2, monitor)
//       |> result.map(process.subject_owner)
//     })
//
//   let assert Ok(_supervisor_subject) =
//     sup.new(sup.OneForOne)
//     |> sup.add(child_worker)
//     |> sup.add(child_worker2)
//     |> sup.start_link
//
//   // Kill the first child to see that it get's demonitored
//   let assert Ok(_) =
//     process.receive(parent_subject, 1000)
//     |> result.map(process.subject_owner(_))
//     |> result.map(process.kill)
//
//   // Receive the second child for validation
//   let assert Ok(child_pid2) =
//     process.receive(parent_subject2, 1000)
//     |> result.map(process.subject_owner)
//
//   // Receive the restarted first child
//   let assert Ok(new_child_pid) =
//     process.receive(parent_subject, 1000)
//     |> result.map(process.subject_owner)
//
//   // Validate monitoring list is correct
//   process.try_call(monitor, monitor_messages.GetState, 10)
//   |> should.be_ok()
//   |> fn(s) {
//     s.monitoring
//     |> list.length()
//     |> should.equal(2)
//
//     s.monitoring
//     |> list.find(fn(m) { m.pid == child_pid2 })
//     |> should.be_ok
//
//     s.monitoring
//     |> list.find(fn(m) { m.pid == new_child_pid })
//     |> should.be_ok
//   }
// }
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
//
// // Nil actor - does nothing but can be monitored
// //---------------
// pub fn new_nil_actor(
//   parent_subject: process.Subject(process.Subject(Nil)),
//   monitor: process.Subject(monitor_messages.Message),
// ) -> Result(process.Subject(Nil), actor.StartError) {
//   actor.start_spec(
//     actor.Spec(
//       init: fn() {
//         let self = process.new_subject()
//         process.send(parent_subject, self)
//
//         monitor.register(monitor, self)
//
//         actor.Ready(
//           Nil,
//           process.new_selector()
//             |> process.selecting(self, function.identity),
//         )
//       },
//       init_timeout: 10,
//       loop: fn(_m: Nil, _s: Nil) -> actor.Next(Nil, Nil) { actor.continue(Nil) },
//     ),
//   )
// }
