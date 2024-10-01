import bg_jobs/job
import bg_jobs/queue
import gleam/dynamic
import gleam/erlang/process
import gleam/int
import gleam/json
import gleam/result
import test_helpers/test_logger

pub const job_name = "FAILING_JOB"

pub fn lookup(logger: process.Subject(test_logger.LogMessage)) {
  #(job_name, worker(logger, _))
}

pub type FailingPayload {
  FailingPayload(message: String)
}

pub fn worker(logger: process.Subject(test_logger.LogMessage), job: job.Job) {
  let assert Ok(FailingPayload(payload)) = from_string(job.payload)
  test_logger.log_message(logger)(
    "Attempt: "
    <> int.to_string(job.attempts)
    <> " - Failed with payload: "
    <> payload,
  )

  Error("I'm always failing")
}

pub fn dispatch(
  queue: process.Subject(queue.Message(a)),
  payload: FailingPayload,
) {
  queue.enqueue_job(queue, job_name, to_string(payload))
}

// Private methods
fn to_string(log_payload: FailingPayload) {
  json.string(log_payload.message)
  |> json.to_string
}

fn from_string(json_string: String) -> Result(FailingPayload, json.DecodeError) {
  json.decode(from: json_string, using: dynamic.string)
  |> result.map(FailingPayload)
}
