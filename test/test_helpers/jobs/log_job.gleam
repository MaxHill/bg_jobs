import bg_jobs/job
import bg_jobs/queue
import gleam/dynamic
import gleam/erlang/process
import gleam/json
import gleam/result
import test_helpers/test_logger

pub const job_name = "LOG_JOB"

pub type Payload {
  Payload(message: String)
}

pub fn worker(logger: process.Subject(test_logger.LogMessage)) {
  queue.Worker(job_name: job_name, execute: execute(logger, _))
}

pub fn execute(logger: process.Subject(test_logger.LogMessage), job: job.Job) {
  let assert Ok(Payload(payload)) = from_string(job.payload)
  test_logger.log_message(logger)(payload)
  Ok(Nil)
}

pub fn dispatch(
  queue: process.Subject(queue.Message(Payload)),
  payload: Payload,
) {
  queue.enqueue_job(queue, job_name, to_string(payload))
}

pub fn to_string(log_payload: Payload) {
  json.string(log_payload.message)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.decode(from: json_string, using: dynamic.string)
  |> result.map(Payload)
}
