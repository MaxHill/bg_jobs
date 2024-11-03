import bg_jobs/db_adapter
import decode
import gleam/dynamic
import gleam/list
import gleam/option
import gleam/result

pub fn decode_erlang_timestamp() {
  decode.into({
    use year <- decode.parameter
    use month <- decode.parameter
    use day <- decode.parameter
    use hour <- decode.parameter
    use second <- decode.parameter
    use minute <- decode.parameter
    #(#(year, month, day), #(hour, minute, second))
  })
  |> decode.subfield([0, 0], decode.int)
  |> decode.subfield([0, 1], decode.int)
  |> decode.subfield([0, 2], decode.int)
  |> decode.subfield([1, 0], decode.int)
  |> decode.subfield([1, 1], decode.int)
  |> decode.subfield([1, 2], decode.int)
}

pub fn transpose_option_to_result(
  value: option.Option(Result(a, b)),
) -> Result(option.Option(a), b) {
  case value {
    option.None -> Ok(option.None)
    option.Some(Ok(a)) -> Ok(option.Some(a))
    option.Some(Error(err)) -> Error(err)
  }
}

pub fn discard_decode(_: dynamic.Dynamic) {
  Ok(Nil)
}

/// Remove in flight jobs to avoid orphaned jobs.
/// This would happen if a queue has active jobs and get's restarted, 
/// either because of a new deploy or a panic
///
pub fn remove_in_flight_jobs(
  queue_name: String,
  db_adapter: db_adapter.DbAdapter,
) {
  db_adapter.get_running_jobs(queue_name)
  |> result.map(fn(jobs) {
    jobs
    |> list.map(fn(job) { job.id })
    |> list.map(db_adapter.release_claim(_))
  })
}
