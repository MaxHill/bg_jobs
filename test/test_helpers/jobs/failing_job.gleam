import bg_jobs
import chip
import gleam/dynamic
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/option
import gleam/result
import test_helpers

pub const job_name = "FAILING_JOB"

pub type Payload {
  FailingPayload(message: String)
}

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  bg_jobs.Worker(job_name: job_name, execute: execute(logger, _))
}

pub fn execute(
  logger: process.Subject(test_helpers.LogMessage),
  job: bg_jobs.Job,
) {
  let assert Ok(FailingPayload(payload)) = from_string(job.payload)
  test_helpers.log_message(logger)(
    "Attempt: "
    <> int.to_string(job.attempts)
    <> " - Failed with payload: "
    <> payload,
  )

  Error("I'm always failing")
}

pub fn dispatch(
  queues: process.Subject(chip.Message(bg_jobs.Message, String, Nil)),
  payload: Payload,
  available_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
) {
  bg_jobs.enqueue_job(queues, job_name, to_string(payload), available_at)
}

// Private methods
pub fn to_string(log_payload: Payload) {
  json.string(log_payload.message)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.decode(from: json_string, using: dynamic.string)
  |> result.map(FailingPayload)
}
