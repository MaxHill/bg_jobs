import birl/duration as birl_duration
import gleam/int
import gleam/list
import gleam/string

pub type Duration {
  Millisecond(Int)
  Second(Int)
  Minute(Int)
  Hour(Int)
  Day(Int)
  Week(Int)
  Month(Int)
  Year(Int)
}

pub fn to_birl(duration: Duration) {
  case duration {
    Millisecond(i) -> birl_duration.milli_seconds(i)
    Second(i) -> birl_duration.seconds(i)
    Minute(i) -> birl_duration.minutes(i)
    Hour(i) -> birl_duration.hours(i)
    Day(i) -> birl_duration.days(i)
    Week(i) -> birl_duration.weeks(i)
    Month(i) -> birl_duration.months(i)
    Year(i) -> birl_duration.years(i)
  }
}

pub type CronSchedule {
  CronSchedule(
    second: TimeValue,
    minute: TimeValue,
    hour: TimeValue,
    day_of_month: TimeValue,
    month: TimeValue,
    day_of_week: TimeValue,
  )
}

pub type TimeValue {
  Every
  Specific(List(TimeSelection))
}

pub type TimeSelection {
  Value(Int)
  Range(Int, Int)
}

pub fn new_schedule() -> CronSchedule {
  CronSchedule(
    second: Specific([Value(0)]),
    minute: Every,
    hour: Every,
    day_of_month: Every,
    month: Every,
    day_of_week: Every,
  )
}

// Second 
//---------------
pub fn every_second(self: CronSchedule) -> CronSchedule {
  CronSchedule(
    Every,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn on_second(self: CronSchedule, value: Int) -> CronSchedule {
  CronSchedule(
    Specific([Value(value)]),
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn on_seconds(self: CronSchedule, value: List(Int)) -> CronSchedule {
  let minutes = value |> list.map(Value)
  CronSchedule(
    Specific(minutes),
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn between_seconds(self: CronSchedule, start: Int, end: Int) -> CronSchedule {
  CronSchedule(
    Specific([Range(start, end)]),
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// MINUTE
//---------------
pub fn every_minute(self: CronSchedule) -> CronSchedule {
  CronSchedule(
    self.second,
    Every,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn on_minute(self: CronSchedule, value: Int) -> CronSchedule {
  CronSchedule(
    self.second,
    Specific([Value(value)]),
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn on_minutes(self: CronSchedule, value: List(Int)) -> CronSchedule {
  let minutes = value |> list.map(Value)
  CronSchedule(
    self.second,
    Specific(minutes),
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn between_minutes(self: CronSchedule, start: Int, end: Int) -> CronSchedule {
  CronSchedule(
    self.second,
    Specific([Range(start, end)]),
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

pub fn every_hour(self: CronSchedule) -> CronSchedule {
  CronSchedule(
    self.second,
    self.minute,
    Every,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// Builder method to set the hour field
pub fn hour(self: CronSchedule, value: Int) -> CronSchedule {
  CronSchedule(
    self.second,
    self.minute,
    Specific([Value(value)]),
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// Builder method to set the day_of_month field
pub fn day_of_month(
  self: CronSchedule,
  value: List(TimeSelection),
) -> CronSchedule {
  CronSchedule(
    self.second,
    self.minute,
    self.hour,
    Specific(value),
    self.month,
    self.day_of_week,
  )
}

// Builder method to set the month field
pub fn month(self: CronSchedule, value: List(TimeSelection)) -> CronSchedule {
  CronSchedule(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    Specific(value),
    self.day_of_week,
  )
}

// Builder method to set the day_of_week field
pub fn day_of_week(
  self: CronSchedule,
  value: List(TimeSelection),
) -> CronSchedule {
  CronSchedule(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    Specific(value),
  )
}

/// Convert the CronSchedule to actual cron syntax
pub fn to_cron_syntax(self: CronSchedule) -> String {
  let second_str = time_value_to_string(self.second)
  let minute_str = time_value_to_string(self.minute)
  let hour_str = time_value_to_string(self.hour)
  let day_of_month_str = time_value_to_string(self.day_of_month)
  let month_str = time_value_to_string(self.month)
  let day_of_week_str = time_value_to_string(self.day_of_week)

  // Combine into the final cron syntax
  second_str
  <> " "
  <> minute_str
  <> " "
  <> hour_str
  <> " "
  <> day_of_month_str
  <> " "
  <> month_str
  <> " "
  <> day_of_week_str
}

/// Helper to convert TimeValue to cron syntax string
fn time_value_to_string(time_value: TimeValue) -> String {
  case time_value {
    Every -> "*"
    Specific(values) ->
      values
      |> list.map(time_selection_to_string)
      |> string.join(",")
  }
}

/// Convert TimeSelection to a string
fn time_selection_to_string(selection: TimeSelection) -> String {
  case selection {
    Value(v) -> int.to_string(v)
    Range(start, end) -> int.to_string(start) <> "-" <> int.to_string(end)
  }
}
