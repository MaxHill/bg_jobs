import bg_jobs/errors
import bg_jobs/internal/bg_jobs_ffi
import decode
import gleam/erlang/process
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

pub fn from_tuple(erl_date: #(#(Int, Int, Int), #(Int, Int, Int))) {
  use date <- result.try(
    date.from_tuple(erl_date.0) |> result.map_error(errors.DateOutOfBoundsError),
  )
  use time <- result.map(
    time.from_tuple(erl_date.1) |> result.map_error(errors.TimeOutOfBoundsError),
  )

  naive_datetime.new(date, time)
}

pub fn pid_to_string(pid: process.Pid) {
  bg_jobs_ffi.pid_to_binary_ffi(pid)
}

pub fn string_to_pid(str: String) {
  bg_jobs_ffi.binary_to_pid_ffi(str)
}
