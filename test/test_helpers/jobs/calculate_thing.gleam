import bg_jobs/job
import bg_jobs/queue
import gleam/dynamic
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json

pub const job_name = "SEND_EMAIL_JOB"

pub const lookup = #(job_name, worker)

pub type CalculateThingPayload {
  CalculateThingJob(first: Int, second: Int)
}

pub fn worker(job: job.Job) {
  let assert Ok(payload) = from_string(job.payload)
  io.debug("Calculated: " <> int.to_string(payload.first + payload.second))
  Ok(Nil)
}

pub fn dispatch(
  queue: process.Subject(queue.Message(a)),
  payload: CalculateThingPayload,
) {
  queue.enqueue_job(queue, job_name, to_string(payload))
}

// Private methods
fn to_string(job: CalculateThingPayload) {
  json.object([
    #("first", json.int(job.first)),
    #("second", json.int(job.second)),
  ])
  |> json.to_string
}

fn from_string(
  json_string: String,
) -> Result(CalculateThingPayload, json.DecodeError) {
  json.decode(
    from: json_string,
    using: dynamic.decode2(
      CalculateThingJob,
      dynamic.field("first", of: dynamic.int),
      dynamic.field("second", of: dynamic.int),
    ),
  )
}
