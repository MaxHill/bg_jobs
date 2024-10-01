import bg_jobs/internal/utils
import decode
import gleam/dynamic
import gleam/option
import gleeunit/should

pub fn parse_timestamps_test() {
  utils.decode_erlang_timestamp()
  |> decode.from(dynamic.from(#(#(1, 1, 1), #(1, 1, 1))))
  |> should.be_ok()
  |> should.equal(#(#(1, 1, 1), #(1, 1, 1)))
}

pub fn transpose_option_to_result_test() {
  option.Some(Ok("yes"))
  |> utils.transpose_option_to_result
  |> should.be_ok
  |> should.be_some
  |> should.equal("yes")

  option.None
  |> utils.transpose_option_to_result
  |> should.be_ok
  |> should.be_none

  option.Some(Error("yes"))
  |> utils.transpose_option_to_result
  |> should.be_error
  |> should.equal("yes")
}
