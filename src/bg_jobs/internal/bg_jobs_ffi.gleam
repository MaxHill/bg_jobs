@external(erlang, "Elixir.BgJobsFfi", "next_run_date")
pub fn get_next_run_date(
  cron: String,
  now: #(#(Int, Int, Int), #(Int, Int, Int)),
) -> #(#(Int, Int, Int), #(Int, Int, Int))
