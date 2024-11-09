import bg_jobs/jobs
import bg_jobs/scheduled_job
import bg_jobs/sqlite_db_adapter
import birl/duration as birl_duration
import gleam/erlang/process
import gleam/list
import gleeunit/should
import sqlight
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
