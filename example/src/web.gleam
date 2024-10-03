import gleam/string_builder
import jobs
import jobs/send_email
import mist
import wisp
import wisp/wisp_mist

pub fn setup_web(secret_key_base, queues) {
  // Start the Mist web server.
  let assert Ok(_) =
    wisp_mist.handler(handle_request(_, queues), secret_key_base)
    |> mist.new
    |> mist.port(8000)
    |> mist.start_http
}

pub fn handle_request(req: wisp.Request, queues) -> wisp.Response {
  case wisp.path_segments(req) {
    [] -> get_home_page(req, queues)
    _ -> wisp.not_found()
  }
}

fn get_home_page(_req: wisp.Request, queues) -> wisp.Response {
  // Dispatch job
  let assert Ok(job) =
    jobs.dispatch_send_email(
      queues,
      send_email.SendEmailPayload(to: "Max", message: "job well done!"),
    )

  wisp.html_response(
    string_builder.from_string("Dispatched job with id: " <> job.id),
    200,
  )
}
