import bg_jobs/errors
import bg_jobs/jobs
import gleam/erlang/process

pub type Message {
  EnqueueJob(
    reply_with: process.Subject(Result(jobs.Job, errors.BgJobError)),
    name: String,
    payload: String,
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
  HandleDispatchError(jobs.Job, exception: String)
}
