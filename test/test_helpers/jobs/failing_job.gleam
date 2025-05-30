import bg_jobs
import bg_jobs/jobs
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/json
import test_helpers

pub const job_name = "FAILING_JOB"

pub type Payload {
  FailingPayload(message: String)
}

fn encode_payload(payload: Payload) -> json.Json {
  let FailingPayload(message:) = payload
  json.object([#("message", json.string(message))])
}

fn payload_decoder() -> decode.Decoder(Payload) {
  use message <- decode.field("message", decode.string)
  decode.success(FailingPayload(message:))
}

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  jobs.Worker(job_name: job_name, handler: handler(logger, _))
}

pub fn handler(logger: process.Subject(test_helpers.LogMessage), job: jobs.Job) {
  let assert Ok(FailingPayload(payload)) = from_string(job.payload)
  test_helpers.log_message(logger)(
    "Attempt: "
    <> int.to_string(job.attempts)
    <> " - Failed with payload: "
    <> payload,
  )

  Error("I'm always failing")
}

pub fn dispatch(bg: bg_jobs.BgJobs, payload: Payload) {
  jobs.new(job_name, to_string(payload))
  |> bg_jobs.enqueue(bg)
}

// Private methods
pub fn to_string(log_payload: Payload) {
  encode_payload(log_payload)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.parse(from: json_string, using: payload_decoder())
}
