import bg_jobs/jobs

pub type Message {
  StartPolling
  StopPolling
  WaitBetweenPoll
  HandleError(jobs.Job, exception: String)
  HandleSuccess(jobs.Job)
  ProcessJobs
}
