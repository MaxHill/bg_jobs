import bg_jobs
import bg_jobs/jobs
import gleam/dynamic
import gleam/erlang/process
import gleam/json
import gleam/result
import test_helpers

pub const job_name = "LOG_JOB"

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  jobs.Worker(job_name: job_name, handler: handler(logger, _))
}

pub fn handler(logger: process.Subject(test_helpers.LogMessage), job: jobs.Job) {
  let assert Ok(Payload(payload)) = from_string(job.payload)
  test_helpers.log_message(logger)(payload)
  Ok(Nil)
}

pub type Payload {
  Payload(message: String)
}

pub fn dispatch(bg: bg_jobs.BgJobs, payload: Payload) {
  bg_jobs.new_job(job_name, to_string(payload))
  |> bg_jobs.enqueue_job(bg)
}

pub fn to_string(log_payload: Payload) {
  json.string(log_payload.message)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.decode(from: json_string, using: dynamic.string)
  |> result.map(Payload)
}
