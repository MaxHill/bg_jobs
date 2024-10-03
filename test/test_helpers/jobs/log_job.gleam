import bg_jobs/job
import bg_jobs/queue
import gleam/dynamic
import gleam/erlang/process
import gleam/json
import gleam/result
import test_helpers/test_logger

pub const job_name = "LOG_JOB"

pub fn lookup(logger: process.Subject(test_logger.LogMessage)) {
  #(job_name, worker(logger, _))
}

pub type LogPayload {
  LogPayload(message: String)
}

pub fn worker(logger: process.Subject(test_logger.LogMessage), job: job.Job) {
  let assert Ok(LogPayload(payload)) = from_string(job.payload)
  test_logger.log_message(logger)(payload)
  Ok(Nil)
}

pub fn dispatch(
  queue: process.Subject(queue.Message(LogPayload)),
  payload: LogPayload,
) {
  queue.enqueue_job(queue, job_name, to_string(payload))
}

// Private methods
fn to_string(log_payload: LogPayload) {
  json.string(log_payload.message)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(LogPayload, json.DecodeError) {
  json.decode(from: json_string, using: dynamic.string)
  |> result.map(LogPayload)
}
