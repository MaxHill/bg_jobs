import bg_jobs/jobs
import bg_jobs/scheduled_job
import bg_jobs/sqlite_db_adapter
import birl/duration as birl_duration
import gleam/erlang/process
import gleam/list
import gleeunit/should
import sqlight
import tempo/naive_datetime
import test_helpers
import test_helpers/jobs as jobs_setup
import test_helpers/jobs/log_job_interval

pub fn test_schedule_intervals() {
  should.equal(
    scheduled_job.new_interval_milliseconds(500),
    scheduled_job.Interval(scheduled_job.Millisecond(500)),
  )
  should.equal(
    scheduled_job.new_interval_seconds(10),
    scheduled_job.Interval(scheduled_job.Second(10)),
  )
  should.equal(
    scheduled_job.new_interval_minutes(5),
    scheduled_job.Interval(scheduled_job.Minute(5)),
  )
  should.equal(
    scheduled_job.new_interval_hours(2),
    scheduled_job.Interval(scheduled_job.Hour(2)),
  )
  should.equal(
    scheduled_job.new_interval_days(1),
    scheduled_job.Interval(scheduled_job.Day(1)),
  )
  should.equal(
    scheduled_job.new_interval_weeks(1),
    scheduled_job.Interval(scheduled_job.Week(1)),
  )
  should.equal(
    scheduled_job.new_interval_months(6),
    scheduled_job.Interval(scheduled_job.Month(6)),
  )
  should.equal(
    scheduled_job.new_interval_years(1),
    scheduled_job.Interval(scheduled_job.Year(1)),
  )
}

pub fn single_interval_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(_bg, db_adapter, logger, _event_logger) <- jobs_setup.setup_interval(
    conn,
  )

  // Wait for jobs to process
  process.sleep(200)

  // Make sure the scheduled job succeeded
  test_helpers.get_log(logger)
  |> list.find(fn(log) { log == "test_log" })
  |> should.equal(Ok("test_log"))

  let _ =
    db_adapter.get_succeeded_jobs(1)
    |> should.be_ok()
    |> list.first()
    |> should.be_ok()

  test_helpers.get_log(logger)
  |> list.find(fn(log) { log == "Attempt: 2" })
  |> should.equal(Ok("Attempt: 2"))

  let _ =
    db_adapter.get_failed_jobs(1)
    |> should.be_ok()
    |> list.first()
    |> should.be_ok()

  Nil
}

pub fn schedule_test() {
  use conn <- sqlight.with_connection(":memory:")
  let logger = test_helpers.new_logger()
  use #(_bg, _db_adapter, _event_logger) <- jobs_setup.setup_schedule(
    conn,
    scheduled_job.new(
      log_job_interval.worker(logger),
      scheduled_job.new_interval_milliseconds(10),
    ),
  )

  process.sleep(200)

  let job_list =
    sqlight.query(
      "SELECT * FROM JOBS",
      conn,
      [],
      sqlite_db_adapter.decode_enqueued_db_row,
    )
    |> should.be_ok
    |> list.map(should.be_ok)

  job_list
  |> list.length
  |> should.equal(1)

  job_list
  |> list.first
  |> should.be_ok
  |> fn(job: jobs.Job) { job.name |> should.equal("LOG_JOB_SCHEDULE") }
}

// Schedule
//---------------

pub fn to_birl_test() {
  should.equal(
    birl_duration.milli_seconds(100),
    scheduled_job.to_birl(scheduled_job.Millisecond(100)),
  )
  should.equal(
    birl_duration.seconds(10),
    scheduled_job.to_birl(scheduled_job.Second(10)),
  )
  should.equal(
    birl_duration.minutes(5),
    scheduled_job.to_birl(scheduled_job.Minute(5)),
  )
  should.equal(
    birl_duration.hours(2),
    scheduled_job.to_birl(scheduled_job.Hour(2)),
  )
  should.equal(
    birl_duration.days(1),
    scheduled_job.to_birl(scheduled_job.Day(1)),
  )
  should.equal(
    birl_duration.weeks(1),
    scheduled_job.to_birl(scheduled_job.Week(1)),
  )
  should.equal(
    birl_duration.months(6),
    scheduled_job.to_birl(scheduled_job.Month(6)),
  )
  should.equal(
    birl_duration.years(1),
    scheduled_job.to_birl(scheduled_job.Year(1)),
  )
}

pub fn new_schedule_test() {
  let schedule = scheduled_job.new_schedule()
  should.equal("0 * * * * *", scheduled_job.to_cron_syntax(schedule))
}

// Second
//---------------
pub fn every_second_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.every_second
  should.equal("* * * * * *", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_second_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_second(30)
  should.equal("0,30 * * * * *", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_seconds_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_seconds(10, 20)
  should.equal("0,10-20 * * * * *", scheduled_job.to_cron_syntax(schedule))
}

// Minute
//---------------
pub fn on_minute_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_minute(30)
  should.equal("0 30 * * * *", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_minutes_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_minutes(5, 10)
  should.equal("0 5-10 * * * *", scheduled_job.to_cron_syntax(schedule))
}

// Hour
//---------------
pub fn on_hour_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_hour(9)
  should.equal("0 * 9 * * *", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_hours_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_hours(8, 12)
  should.equal("0 * 8-12 * * *", scheduled_job.to_cron_syntax(schedule))
}

// Day of month
//---------------
pub fn on_day_of_month_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.on_day_of_month(15)
  should.equal("0 * * 15 * *", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_day_of_months_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_day_of_months(1, 15)
  should.equal("0 * * 1-15 * *", scheduled_job.to_cron_syntax(schedule))
}

// Month
//---------------
pub fn on_month_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_month(5)
  should.equal("0 * * * 5 *", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_months_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_months(3, 8)
  should.equal("0 * * * 3-8 *", scheduled_job.to_cron_syntax(schedule))
}

// Day of week
//---------------
pub fn on_day_of_week_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_day_of_week(3)
  should.equal("0 * * * * 3", scheduled_job.to_cron_syntax(schedule))
}

pub fn between_day_of_weeks_test() {
  let schedule =
    scheduled_job.new_schedule() |> scheduled_job.between_day_of_weeks(1, 5)
  should.equal("0 * * * * 1-5", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_mondays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_mondays
  should.equal("0 * * * * 1", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_tuesdays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_thuesdays
  should.equal("0 * * * * 2", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_wednesdays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_wednesdays
  should.equal("0 * * * * 3", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_thursdays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_thursdays
  should.equal("0 * * * * 4", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_fridays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_fridays
  should.equal("0 * * * * 5", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_saturdays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_saturdays
  should.equal("0 * * * * 6", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_sundays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_sundays
  should.equal("0 * * * * 7", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_weekdays_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_weekdays
  should.equal("0 * * * * 1,2,3,4,5", scheduled_job.to_cron_syntax(schedule))
}

pub fn on_weekends_test() {
  let schedule = scheduled_job.new_schedule() |> scheduled_job.on_weekends
  should.equal("0 * * * * 6,7", scheduled_job.to_cron_syntax(schedule))
}

pub fn next_must_be_in_future_test() {
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule() |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:01:00")
  |> scheduled_job.next_run_date(schedule)
  |> should.equal(#(#(2024, 01, 01), #(01, 02, 00)))
}

pub fn align_seconds_test() {
  let now = naive_datetime.literal("2024-01-01T01:01:01.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.every_second()
    |> scheduled_job.on_second(30)
    |> scheduled_job.build_schedule()

  scheduled_job.align_seconds(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:01:30.009"))

  let now = naive_datetime.literal("2024-01-01T01:01:31.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.every_second()
    |> scheduled_job.on_second(30)
    |> scheduled_job.build_schedule()

  scheduled_job.align_seconds(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:02:30.009"))

  // Should return the same date 
  // since seconds is matching a range between 4 and 6
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.every_second()
    |> scheduled_job.between_seconds(4, 6)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:01:05.009")
  |> scheduled_job.align_seconds(schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:01:05.009"))
}

pub fn align_minutes_test() {
  let now = naive_datetime.literal("2024-01-01T01:01:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_minute(30)
    |> scheduled_job.build_schedule()

  scheduled_job.align_minutes(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:30:00.009"))

  let now = naive_datetime.literal("2024-01-01T01:31:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_minute(30)
    |> scheduled_job.build_schedule()

  scheduled_job.align_minutes(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T02:30:00.009"))

  // Should return the same date 
  // since minute is matching a range between 4 and 6
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.between_minutes(4, 6)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:05:00.009")
  |> scheduled_job.align_minutes(schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:05:00.009"))
}

pub fn align_hours_test() {
  let now = naive_datetime.literal("2024-01-01T01:01:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_hour(4)
    |> scheduled_job.build_schedule()

  scheduled_job.align_hours(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T04:01:00.009"))

  let now = naive_datetime.literal("2024-01-01T05:01:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_hour(4)
    |> scheduled_job.build_schedule()

  scheduled_job.align_hours(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-02T04:01:00.009"))

  // Should return the same date 
  // since hour is matching a range between 4 and 6
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.between_hours(4, 6)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T05:01:00.009")
  |> scheduled_job.align_hours(schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T05:01:00.009"))
}

pub fn align_day_of_month_test() {
  // Should return the same date
  // since both day_of_week and day_of_month match every
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule() |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-01T01:01:00.009"))

  // Should have incremented days 
  // since day_of_month matches specific 4
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_day_of_month(4)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-04T01:01:00.009"))

  // Should have incremented days and month 
  // since day_of_month is matching specific 4 and it's starting on the 5th
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_day_of_month(4)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-05T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-02-04T01:01:00.009"))

  // Should return the same date 
  // since day_of_month is matching a range between 4 and 6
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.between_day_of_months(4, 6)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-05T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-05T01:01:00.009"))

  // Should have incremented days 
  // since day_of_week is 4 and it's starting on a monday
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_day_of_week(4)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-01T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-04T01:01:00.009"))

  // Should have incremented days and months
  // since day_of_week is 4 and it's starting on a Thursday
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_day_of_week(4)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-05T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-11T01:01:00.009"))

  // Should return the same date 
  // since day_of_week is matching a range between 4 and 6
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.between_day_of_months(4, 6)
    |> scheduled_job.build_schedule()

  naive_datetime.literal("2024-01-05T01:01:00.009")
  |> scheduled_job.align_day(schedule)
  |> should.equal(naive_datetime.literal("2024-01-05T01:01:00.009"))
}

pub fn align_months() {
  let now = naive_datetime.literal("2024-01-01T01:01:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_month(4)
    |> scheduled_job.build_schedule()

  scheduled_job.align_month(now, schedule)
  |> should.equal(naive_datetime.literal("2024-01-04T01:01:00.009"))

  let now = naive_datetime.literal("2024-05-01T01:01:00.009")
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_month(4)
    |> scheduled_job.build_schedule()

  scheduled_job.align_month(now, schedule)
  |> should.equal(naive_datetime.literal("2025-04-01T01:01:00.009"))

  // Test leap year
  let now = naive_datetime.literal("2024-02-28T23:59:59.999")
  // Last moment of February in leap year
  let assert Ok(scheduled_job.Schedule(schedule)) =
    scheduled_job.new_schedule()
    |> scheduled_job.on_month(3)
    |> scheduled_job.build_schedule()

  scheduled_job.align_month(now, schedule)
  |> should.equal(naive_datetime.literal("2024-03-01T23:59:59.999"))
}

// Build schedule test
//---------------

pub fn success_build_schedule_second_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_second(3)
  |> scheduled_job.between_seconds(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
      minute: scheduled_job.Every,
      hour: scheduled_job.Every,
      day_of_month: scheduled_job.Every,
      month: scheduled_job.Every,
      day_of_week: scheduled_job.Every,
    )),
  )
}

pub fn error_build_schedule_second_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_second(-1)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Seconds must be between 0 and 60, found: -1",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_second(61)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Seconds must be between 0 and 60, found: 61",
  ))
}

pub fn success_build_schedule_minute_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_minute(3)
  |> scheduled_job.between_minutes(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Every,
      minute: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
      hour: scheduled_job.Every,
      day_of_month: scheduled_job.Every,
      month: scheduled_job.Every,
      day_of_week: scheduled_job.Every,
    )),
  )
}

pub fn error_build_schedule_minute_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.on_minute(-1)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Minutes must be between 0 and 60, found: -1",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.on_minute(61)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Minutes must be between 0 and 60, found: 61",
  ))
}

pub fn success_build_schedule_hour_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_hour(3)
  |> scheduled_job.between_hours(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Every,
      minute: scheduled_job.Every,
      hour: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
      day_of_month: scheduled_job.Every,
      month: scheduled_job.Every,
      day_of_week: scheduled_job.Every,
    )),
  )
}

pub fn error_build_schedule_hour_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.on_hour(-1)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Hours must be between 0 and 24, found: -1",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.on_hour(25)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Hours must be between 0 and 24, found: 25",
  ))
}

pub fn success_build_schedule_day_of_month_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_day_of_month(3)
  |> scheduled_job.between_day_of_months(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Every,
      minute: scheduled_job.Every,
      hour: scheduled_job.Every,
      day_of_month: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
      month: scheduled_job.Every,
      day_of_week: scheduled_job.Every,
    )),
  )
}

pub fn error_build_schedule_day_of_month_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.on_day_of_month(0)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Day of month must be between 1 and 31, found: 0",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.on_day_of_month(32)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Day of month must be between 1 and 31, found: 32",
  ))
}

pub fn success_build_schedule_month_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_month(3)
  |> scheduled_job.between_months(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Every,
      minute: scheduled_job.Every,
      hour: scheduled_job.Every,
      day_of_month: scheduled_job.Every,
      month: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
      day_of_week: scheduled_job.Every,
    )),
  )
}

pub fn error_build_schedule_month_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.on_month(0)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Month must be between 1 and 12, found: 0",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.on_month(13)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Month must be between 1 and 12, found: 13",
  ))
}

pub fn success_build_schedule_day_of_week_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.every_second()
  |> scheduled_job.on_day_of_week(3)
  |> scheduled_job.between_day_of_weeks(3, 5)
  |> scheduled_job.build_schedule()
  |> should.be_ok()
  |> should.equal(
    scheduled_job.Schedule(scheduled_job.DateSchedule(
      second: scheduled_job.Every,
      minute: scheduled_job.Every,
      hour: scheduled_job.Every,
      day_of_month: scheduled_job.Every,
      month: scheduled_job.Every,
      day_of_week: scheduled_job.Specific([
        scheduled_job.Value(3),
        scheduled_job.Range(3, 5),
      ]),
    )),
  )
}

pub fn error_build_schedule_day_of_week_test() {
  scheduled_job.new_schedule()
  |> scheduled_job.on_day_of_week(0)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Day of week must be between 1 and 7, found: 0",
  ))

  scheduled_job.new_schedule()
  |> scheduled_job.on_day_of_week(8)
  |> scheduled_job.build_schedule()
  |> should.be_error()
  |> should.equal(scheduled_job.OutOfBoundsError(
    "Day of week must be between 1 and 7, found: 8",
  ))
}
