import gleam/erlang/process
import gleam/string_builder
import jobs
import sqlight
import web
import wisp

pub fn main() {
  use conn <- sqlight.with_connection(":memory:")
  let #(queues, _supervisor) = jobs.setup_queues(conn)

  let secret_key_base = wisp.random_string(64)

  let assert Ok(_web_supervisor) = web.setup_web(secret_key_base, queues)

  process.sleep_forever()
}

pub fn handle_request(req: wisp.Request) -> wisp.Response {
  case wisp.path_segments(req) {
    [] -> get_home_page(req)
    _ -> wisp.not_found()
  }
}

fn get_home_page(_req: wisp.Request) -> wisp.Response {
  wisp.html_response(string_builder.from_string("Hello"), 200)
}
