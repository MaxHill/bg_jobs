import bg_jobs
import bg_jobs/jobs
import gleam/dynamic
import gleam/io
import gleam/json
import gleam/result

pub const job_name = "SEND_EMAIL"

pub type Payload {
  Payload(to: String, message: String)
}

pub fn worker() {
  jobs.Worker(job_name: job_name, handler: handler)
}

pub fn handler(job: jobs.Job) {
  use payload <- result.try(
    from_string(job.payload)
    |> result.map_error(fn(_e) { "Could not decode payload" }),
  )

  io.debug(
    "Sending email to: " <> payload.to <> " with message: " <> payload.message,
  )
  Ok(Nil)
}

pub fn dispatch(bg: bg_jobs.BgJobs, to to: String, message message: String) {
  jobs.new(job_name, to_string(Payload(to, message)))
  |> bg_jobs.enqueue_job(bg)
}

fn to_string(send_email_job: Payload) {
  json.object([
    #("to", json.string(send_email_job.to)),
    #("message", json.string(send_email_job.message)),
  ])
  |> json.to_string
}

fn from_string(json_string: String) -> Result(Payload, json.DecodeError) {
  json.decode(
    from: json_string,
    using: dynamic.decode2(
      Payload,
      dynamic.field("to", of: dynamic.string),
      dynamic.field("message", of: dynamic.string),
    ),
  )
}
