import bg_jobs/internal/time
import bg_jobs_ffi
import birl
import birl/duration
import gleam/io
import gleeunit/should

pub fn get_next_run_date_test() {
  let date =
    birl.from_erlang_universal_datetime(#(#(2024, 01, 01), #(00, 00, 01)))
  let next_run =
    birl.TimeOfDay(..birl.get_time_of_day(date), second: 0)
    |> birl.set_time_of_day(date, _)
    |> birl.add(duration.minutes(1))
    |> birl.to_erlang_datetime()

  bg_jobs_ffi.get_next_run_date("* * * * *", date |> birl.to_erlang_datetime())
  |> should.equal(next_run)
}

pub fn thing_test() {
  let date =
    birl.from_erlang_universal_datetime(#(#(2024, 01, 01), #(00, 00, 01)))

  time.new_schedule()
  |> time.between_minutes(15, 16)
  |> time.to_cron_syntax()
  |> bg_jobs_ffi.get_next_run_date(birl.to_erlang_datetime(date))
}

pub fn schedule_seconds_test() {
  time.new_schedule()
  |> time.on_seconds([1, 2, 3, 5])
  |> time.to_cron_syntax()
  |> should.equal("1,2,3,5 * * * * *")

  time.new_schedule()
  |> time.on_second(2)
  |> time.to_cron_syntax()
  |> should.equal("2 * * * * *")

  time.new_schedule()
  |> time.every_second()
  |> time.to_cron_syntax()
  |> should.equal("* * * * * *")

  time.new_schedule()
  |> time.between_seconds(1, 12)
  |> time.to_cron_syntax()
  |> should.equal("1-12 * * * * *")
}

pub fn schedule_happy_path_test() {
  time.new_schedule()
  |> time.on_minutes([1, 2, 3, 5])
  |> fn(schedule) {
    schedule |> time.to_cron_syntax() |> should.equal("0 1,2,3,5 * * * *")
    schedule
  }
  |> time.on_minute(2)
  |> fn(schedule) {
    schedule |> time.to_cron_syntax() |> should.equal("0 2 * * * *")
    schedule
  }
  |> time.every_minute()
  |> fn(schedule) {
    schedule |> time.to_cron_syntax() |> should.equal("0 * * * * *")
    schedule
  }
  |> time.between_minutes(1, 12)
  |> fn(schedule) {
    schedule |> time.to_cron_syntax() |> should.equal("0 1-12 * * * *")
    schedule
  }
}
