import bg_jobs/jobs
import gleam/erlang/process
import test_helpers

pub const job_name = "FOREVER_JOB_SCHEDULE"

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  jobs.Worker(job_name: job_name, handler: handler(logger, _))
}

pub fn handler(logger: process.Subject(test_helpers.LogMessage), _job: jobs.Job) {
  test_helpers.log_message(logger)("Running forever...")
  process.sleep_forever()
  Ok(Nil)
}
