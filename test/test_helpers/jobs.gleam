import bg_jobs/queue
import bg_jobs/sqlite_store
import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervisor
import gleam/result
import sqlight
import test_helpers
import test_helpers/jobs/calculate_thing
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/jobs/send_email
import test_helpers/test_logger

pub type DefaultJobs {
  SendEmail(send_email.SendEmailPayload)
  CalculateThing(calculate_thing.CalculateThingPayload)
}

pub fn setup(conn: sqlight.Connection) {
  use _ <- result.map(test_helpers.reset_db(conn))
  let job_store = sqlite_store.try_new_store(conn)
  let queue_name = "test-queue"

  let logger = test_logger.new_logger()

  // Setup suprevisor tree
  let parent_subject = process.new_subject()

  let queue =
    supervisor.worker(queue.start(
      _,
      parent_subject,
      queue_name: queue_name,
      max_retries: 3,
      job_store: job_store,
      job_mapper: queue.match_worker([
        send_email.lookup,
        calculate_thing.lookup,
        log_job.lookup(logger),
        failing_job.lookup(logger),
      ]),
    ))

  let assert Ok(_supervisor_subject) =
    supervisor.start(supervisor.add(_, queue))

  let assert Ok(queue) = process.receive(parent_subject, 1000)

  #(queue, queue_name, job_store, logger)
}
