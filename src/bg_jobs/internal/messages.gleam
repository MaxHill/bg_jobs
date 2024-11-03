import bg_jobs/internal/errors
import bg_jobs/internal/jobs
import gleam/erlang/process

pub type QueueMessage {
  StartPolling
  StopPolling
  WaitBetweenPoll
  HandleError(jobs.Job, exception: String)
  HandleSuccess(jobs.Job)
  ProcessJobs
}

pub type DispatcherMessage {
  EnqueueJob(
    reply_with: process.Subject(Result(jobs.Job, errors.BgJobError)),
    name: String,
    payload: String,
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
  HandleDispatchError(jobs.Job, exception: String)
}
