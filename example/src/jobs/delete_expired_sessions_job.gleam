import bg_jobs/jobs
import gleam/io

pub const job_name = "DELETE_EXPIRED_SESSIONS"

pub fn worker() {
  jobs.Worker(job_name: job_name, handler: handler)
}

pub fn handler(_: jobs.Job) {
  io.debug("Deleting expired sessions...")
  Ok(Nil)
}
