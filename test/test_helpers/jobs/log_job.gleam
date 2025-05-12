import bg_jobs
import bg_jobs/jobs
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/json
import test_helpers

pub const job_name = "LOG_JOB"

pub type Payload {
  Payload(message: String)
}

fn encode_payload(payload: Payload) -> json.Json {
  let Payload(message:) = payload
  json.object([#("message", json.string(message))])
}

fn payload_decoder() -> decode.Decoder(Payload) {
  use message <- decode.field("message", decode.string)
  decode.success(Payload(message:))
}

pub fn worker(logger: process.Subject(test_helpers.LogMessage)) {
  jobs.Worker(job_name: job_name, handler: handler(logger, _))
}

pub fn handler(logger: process.Subject(test_helpers.LogMessage), job: jobs.Job) {
  let assert Ok(Payload(payload)) = from_string(job.payload)
  test_helpers.log_message(logger)(payload)
  Ok(Nil)
}

pub fn dispatch(bg: bg_jobs.BgJobs, payload: Payload) {
  bg_jobs.new_job(job_name, to_string(payload))
  |> bg_jobs.enqueue(bg)
}

pub fn to_string(log_payload: Payload) {
  encode_payload(log_payload)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.parse(from: json_string, using: payload_decoder())
}
