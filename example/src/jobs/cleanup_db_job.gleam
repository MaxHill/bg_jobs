import bg_jobs/jobs
import gleam/io

pub const job_name = "CLEANUP_DB"

pub fn worker() {
  jobs.Worker(job_name: job_name, handler: handler)
}

pub fn handler(_: jobs.Job) {
  io.debug("Cleaning up the db...")
  Ok(Nil)
}
