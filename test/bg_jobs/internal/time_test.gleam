import bg_jobs/internal/time
import bg_jobs_ffi
import birl
import birl/duration as birl_duration
import gleeunit/should

pub fn test_to_birl() {
  should.equal(
    birl_duration.milli_seconds(100),
    time.to_birl(time.Millisecond(100)),
  )
  should.equal(birl_duration.seconds(10), time.to_birl(time.Second(10)))
  should.equal(birl_duration.minutes(5), time.to_birl(time.Minute(5)))
  should.equal(birl_duration.hours(2), time.to_birl(time.Hour(2)))
  should.equal(birl_duration.days(1), time.to_birl(time.Day(1)))
  should.equal(birl_duration.weeks(1), time.to_birl(time.Week(1)))
  should.equal(birl_duration.months(6), time.to_birl(time.Month(6)))
  should.equal(birl_duration.years(1), time.to_birl(time.Year(1)))
}

pub fn test_cron_schedule_builders() {
  let schedule1 = time.new_schedule() |> time.every_second
  should.equal(schedule1.second, time.Every)

  let schedule2 = time.new_schedule() |> time.on_second(15)
  should.equal(schedule2.second, time.Specific([time.Value(15)]))

  let schedule3 = time.new_schedule() |> time.on_seconds([10, 20, 30])
  should.equal(
    schedule3.second,
    time.Specific([time.Value(10), time.Value(20), time.Value(30)]),
  )

  let schedule4 = time.new_schedule() |> time.between_seconds(5, 15)
  should.equal(schedule4.second, time.Specific([time.Range(5, 15)]))

  let schedule5 = time.new_schedule() |> time.every_minute
  should.equal(schedule5.minute, time.Every)

  let schedule6 = time.new_schedule() |> time.on_minute(45)
  should.equal(schedule6.minute, time.Specific([time.Value(45)]))

  let schedule7 = time.new_schedule() |> time.on_minutes([10, 20])
  should.equal(
    schedule7.minute,
    time.Specific([time.Value(10), time.Value(20)]),
  )

  let schedule8 = time.new_schedule() |> time.between_minutes(10, 30)
  should.equal(schedule8.minute, time.Specific([time.Range(10, 30)]))

  let schedule9 = time.new_schedule() |> time.every_hour
  should.equal(schedule9.hour, time.Every)

  let schedule10 = time.new_schedule() |> time.hour(3)
  should.equal(schedule10.hour, time.Specific([time.Value(3)]))

  let schedule11 =
    time.new_schedule()
    |> time.day_of_month([time.Value(1), time.Range(10, 15)])
  should.equal(
    schedule11.day_of_month,
    time.Specific([time.Value(1), time.Range(10, 15)]),
  )

  let schedule12 = time.new_schedule() |> time.month([time.Value(6)])
  should.equal(schedule12.month, time.Specific([time.Value(6)]))

  let schedule13 =
    time.new_schedule() |> time.day_of_week([time.Value(2), time.Range(4, 5)])
  should.equal(
    schedule13.day_of_week,
    time.Specific([time.Value(2), time.Range(4, 5)]),
  )
}

pub fn test_to_cron_syntax() {
  let default_schedule = time.new_schedule()
  should.equal(time.to_cron_syntax(default_schedule), "0 * * * * *")

  // Test custom schedule with specific seconds, minutes, and hours
  let custom_schedule =
    time.new_schedule()
    |> time.on_second(30)
    |> time.on_minute(15)
    |> time.hour(2)
    |> time.day_of_month([time.Value(1), time.Range(10, 20)])
    |> time.month([time.Value(6)])
    |> time.day_of_week([time.Value(3)])
  should.equal(time.to_cron_syntax(custom_schedule), "30 15 2 1,10-20 6 3")

  // Test every second and every minute
  let every_second_minute_schedule =
    time.new_schedule()
    |> time.every_second
    |> time.every_minute
  should.equal(time.to_cron_syntax(every_second_minute_schedule), "* * * * * *")

  // Test range and list values in `day_of_week`
  let complex_schedule =
    time.new_schedule()
    |> time.on_minute(0)
    |> time.hour(12)
    |> time.day_of_week([time.Value(1), time.Range(3, 5), time.Value(7)])
  should.equal(time.to_cron_syntax(complex_schedule), "0 0 12 * * 1,3-5,7")
}

pub fn schedule_every_10_seconds_test() {
  let date =
    birl.from_erlang_universal_datetime(#(#(2024, 01, 01), #(00, 00, 01)))

  let next_run = #(#(2024, 01, 01), #(00, 00, 10))

  bg_jobs_ffi.get_next_run_date(
    "10 * * * * *",
    date |> birl.to_erlang_datetime(),
  )
  |> should.equal(next_run)
}

pub fn get_next_run_date_test() {
  let date =
    birl.from_erlang_universal_datetime(#(#(2024, 01, 01), #(00, 00, 01)))

  let next_run = #(#(2024, 01, 01), #(00, 00, 01))

  bg_jobs_ffi.get_next_run_date(
    "* * * * * *",
    date |> birl.to_erlang_datetime(),
  )
  |> should.equal(next_run)
}

pub fn builder_test() {
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
