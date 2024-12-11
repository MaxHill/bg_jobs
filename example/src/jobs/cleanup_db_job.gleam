import bg_jobs/jobs
import wisp

pub const job_name = "CLEANUP_DB"

pub fn worker() {
  jobs.Worker(job_name: job_name, handler: handler)
}

pub fn handler(_: jobs.Job) {
  wisp.log_notice("Cleaning up the db...")
  Ok(Nil)
}
