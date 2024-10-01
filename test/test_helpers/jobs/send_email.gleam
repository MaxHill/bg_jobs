import bg_jobs/job
import bg_jobs/queue
import gleam/dynamic
import gleam/erlang/process
import gleam/io
import gleam/json

pub const job_name = "SEND_EMAIL_JOB"

pub const lookup = #(job_name, worker)

pub type SendEmailPayload {
  SendEmailPayload(to: String, message: String)
}

pub fn worker(job: job.Job) {
  let assert Ok(payload) = from_string(job.payload)
  io.debug(
    "Sending email to: " <> payload.to <> " with message: " <> payload.message,
  )
  Ok(Nil)
}

pub fn dispatch(
  queue: process.Subject(queue.Message(a)),
  payload: SendEmailPayload,
) {
  queue.enqueue_job(queue, job_name, to_string(payload))
}

// Private methods
fn to_string(send_email_job: SendEmailPayload) {
  json.object([
    #("to", json.string(send_email_job.to)),
    #("message", json.string(send_email_job.message)),
  ])
  |> json.to_string
}

fn from_string(
  json_string: String,
) -> Result(SendEmailPayload, json.DecodeError) {
  json.decode(
    from: json_string,
    using: dynamic.decode2(
      SendEmailPayload,
      dynamic.field("to", of: dynamic.string),
      dynamic.field("message", of: dynamic.string),
    ),
  )
}
