import bg_jobs
import bg_jobs/errors
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import gleam/list
import gleam/result
import gleam/string
import gleam/string_builder
import gleam/string_tree
import jobs
import jobs/send_email_job
import mist
import sqlight
import wisp
import wisp/wisp_mist

type Context {
  Context(bg: bg_jobs.BgJobs, conn: sqlight.Connection)
}

pub fn main() {
  use conn <- sqlight.with_connection(":memory:")
  let assert Ok(bg_jobs) = jobs.setup(conn)

  let ctx = Context(bg: bg_jobs, conn:)

  wisp.configure_logger()

  let secret_key_base = wisp.random_string(64)
  // Start the Mist web server.
  let assert Ok(_) =
    wisp_mist.handler(handle_request(_, ctx), secret_key_base)
    |> mist.new
    |> mist.port(8000)
    |> mist.start_http

  process.sleep_forever()
}

fn handle_request(req: wisp.Request, ctx: Context) -> wisp.Response {
  case wisp.path_segments(req) {
    [] -> home_page(req, ctx)
    ["send-email"] -> send_email_page(req, ctx)
    _ -> wisp.not_found()
  }
}

fn home_page(_req: wisp.Request, ctx: Context) {
  let all_jobs =
    sqlight.query(
      "SELECT * FROM jobs",
      ctx.conn,
      [],
      sqlite_db_adapter.decode_enqueued_db_row,
    )
    |> result.map_error(string.inspect)
    |> result.map_error(errors.DbError)

  case all_jobs {
    Ok(jobs) -> {
      let job_list =
        list.map(jobs, fn(job) { "<li>" <> string.inspect(job) <> "</li>" })
        |> string.join("")

      wisp.html_response(string_tree.from_string("
      <ul>
      " <> job_list <> "
      </ul>
      "), 200)
    }
    Error(_) -> {
      wisp.html_response(string_tree.from_string("Could not get jobs"), 500)
    }
  }
}

fn send_email_page(_req: wisp.Request, ctx: Context) {
  let assert Ok(_) =
    send_email_job.dispatch(ctx.bg, to: "max@test.com", message: "hello")
  wisp.redirect("/")
}
