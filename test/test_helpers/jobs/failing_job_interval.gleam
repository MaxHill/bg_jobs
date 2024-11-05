import bg_jobs/jobs
import gleam/erlang/process
import gleam/int
import test_helpers

pub const job_name = "FAILING_JOB_SCHEDULE"

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  jobs.Worker(job_name: job_name, handler: handler(logger, _))
}

pub fn handler(logger: process.Subject(test_helpers.LogMessage), job: jobs.Job) {
  test_helpers.log_message(logger)("Attempt: " <> int.to_string(job.attempts))

  Error("I'm always failing")
}
