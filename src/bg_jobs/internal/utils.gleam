import bg_jobs/db_adapter
import bg_jobs/errors
import decode
import gleam/dynamic
import gleam/list
import gleam/option
import gleam/result
import tempo/date
import tempo/naive_datetime
import tempo/time

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

pub fn from_tuple(erl_date: #(#(Int, Int, Int), #(Int, Int, Int))) {
  use date <- result.try(
    date.from_tuple(erl_date.0) |> result.map_error(errors.DateOutOfBoundsError),
  )
  use time <- result.map(
    time.from_tuple(erl_date.1) |> result.map_error(errors.TimeOutOfBoundsError),
  )

  naive_datetime.new(date, time)
}

/// Remove in flight jobs to avoid orphaned jobs.
/// This would happen if a queue has active jobs and get's restarted, 
/// either because of a new deploy or a panic
///
pub fn remove_in_flight_jobs(
  queue_name: String,
  db_adapter: db_adapter.DbAdapter,
) {
  db_adapter.get_running_jobs_by_queue_name(queue_name)
  |> result.map(fn(jobs) {
    jobs
    |> list.map(fn(job) { job.id })
    |> list.map(db_adapter.release_claim(_))
  })
}
