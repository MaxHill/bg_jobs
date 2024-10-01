import decode
import gleam/dynamic
import gleam/option

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
