import bg_jobs/jobs
import wisp

pub const job_name = "DELETE_EXPIRED_SESSIONS"

pub fn worker() {
  jobs.Worker(job_name: job_name, handler: handler)
}

pub fn handler(_: jobs.Job) {
  wisp.log_notice("Deleting expired sessions...")
  Ok(Nil)
}
