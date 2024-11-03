import bg_jobs/internal/jobs

pub type Message {
  StartPolling
  StopPolling
  WaitBetweenPoll
  ScheduleNext
  HandleError(jobs.Job, exception: String)
  HandleSuccess(jobs.Job)
  ProcessJobs
}
