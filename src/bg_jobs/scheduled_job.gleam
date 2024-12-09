import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/monitor
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages.{type Message} as messages
import bg_jobs/internal/utils
import bg_jobs/jobs
import chip
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/list
import gleam/order
import gleam/otp/actor
import gleam/result
import tempo
import tempo/date
import tempo/duration
import tempo/month
import tempo/naive_datetime
import tempo/time

pub type Spec {
  Spec(
    schedule: Schedule,
    worker: jobs.Worker,
    max_retries: Int,
    init_timeout: Int,
    poll_interval: Int,
    event_listeners: List(events.EventListener),
  )
}

pub type Schedule {
  Interval(Duration)
  Schedule(DateSchedule)
}

// Interval
//---------------
pub type Duration {
  Millisecond(Int)
  Second(Int)
  Minute(Int)
  Hour(Int)
  Day(Int)
  Week(Int)
}

@internal
pub fn to_gtempo(duration: Duration) {
  case duration {
    Millisecond(i) -> duration.milliseconds(i)
    Second(i) -> duration.seconds(i)
    Minute(i) -> duration.minutes(i)
    Hour(i) -> duration.hours(i)
    Day(i) -> duration.days(i)
    Week(i) -> duration.weeks(i)
  }
}

/// Create interval schedule in milliseconds
pub fn new_interval_milliseconds(milliseconds: Int) {
  Interval(Millisecond(milliseconds))
}

/// Create interval schedule in seconds
pub fn new_interval_seconds(seconds: Int) {
  Interval(Second(seconds))
}

/// Create interval schedule in minutes
pub fn new_interval_minutes(minutes: Int) {
  Interval(Minute(minutes))
}

/// Create interval schedule in hours
pub fn new_interval_hours(hours: Int) {
  Interval(Hour(hours))
}

/// Create interval schedule in days
pub fn new_interval_days(days: Int) {
  Interval(Day(days))
}

/// Create interval schedule in weeks
pub fn new_interval_weeks(weeks: Int) {
  Interval(Week(weeks))
}

// Schdedule
//---------------
pub type DateScheduleBuilder {
  DateScheduleBuilder(
    second: TimeValue,
    minute: TimeValue,
    hour: TimeValue,
    day_of_month: TimeValue,
    month: TimeValue,
    day_of_week: TimeValue,
  )
}

pub type DateSchedule {
  DateSchedule(
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

pub type DateError {
  DateError(tempo.DateOutOfBoundsError)
  TimeError(tempo.TimeOutOfBoundsError)
  OutOfBoundsError(String)
}

/// Create a new schedule that runs on the first second
/// every minut
pub fn new_schedule() -> DateScheduleBuilder {
  DateScheduleBuilder(
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
/// Sets the schedule to trigger every second.
pub fn every_second(self: DateScheduleBuilder) -> DateScheduleBuilder {
  DateScheduleBuilder(
    Every,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

/// Adds a specific second at which the schedule should trigger.
/// If other specific seconds are already set, appends the given second.
/// 
/// Note: the default value is to trigger on second 0 so you would 
/// need to set it to every second first and then to a specific to 
/// only get that 
pub fn on_second(self: DateScheduleBuilder, second: Int) -> DateScheduleBuilder {
  let second = case self.second {
    Every -> Specific([Value(second)])
    Specific(values) -> Specific(list.flatten([values, [Value(second)]]))
  }

  DateScheduleBuilder(
    second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

/// Sets a range of seconds during which the schedule should trigger.
pub fn between_seconds(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let second = case self.second {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// MINUTE
//---------------
/// Adds a specific minute at which the schedule should trigger.
/// If other specific minutes are already set, appends the given minute.
pub fn on_minute(self: DateScheduleBuilder, value: Int) -> DateScheduleBuilder {
  let minute = case self.minute {
    Every -> {
      Specific([Value(value)])
    }
    Specific(values) -> {
      Specific(list.flatten([values, [Value(value)]]))
    }
  }
  DateScheduleBuilder(
    self.second,
    minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

/// Sets a range of minutes during which the schedule should trigger.
pub fn between_minutes(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let minute = case self.minute {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    self.second,
    minute,
    self.hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// HOUR
//---------------
/// Adds a specific hour at which the schedule should trigger.
/// If other specific hours are already set, appends the given hour.
pub fn on_hour(self: DateScheduleBuilder, value: Int) -> DateScheduleBuilder {
  let hour = case self.hour {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

/// Sets a range of hours during which the schedule should trigger.
pub fn between_hours(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let hour = case self.hour {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    hour,
    self.day_of_month,
    self.month,
    self.day_of_week,
  )
}

// DAY OF MONTH
//---------------
// Builder method to set the day_of_month field

/// Adds a specific day of the month on which the schedule should trigger.
/// If other specific days are already set, appends the given day.
pub fn on_day_of_month(
  self: DateScheduleBuilder,
  value: Int,
) -> DateScheduleBuilder {
  let day_of_month = case self.day_of_month {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    day_of_month,
    self.month,
    self.day_of_week,
  )
}

/// Sets a range of days of the month during which the schedule should trigger.
pub fn between_day_of_months(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let day_of_month = case self.day_of_month {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    day_of_month,
    self.month,
    self.day_of_week,
  )
}

// MONTH
//---------------
// Builder method to set the month field

/// Adds a specific month in which the schedule should trigger.
/// If other specific months are already set, appends the given month.
pub fn on_month(self: DateScheduleBuilder, value: Int) -> DateScheduleBuilder {
  let month = case self.month {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    month,
    self.day_of_week,
  )
}

/// Sets a range of months during which the schedule should trigger.
pub fn between_months(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let month = case self.month {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    month,
    self.day_of_week,
  )
}

/// Shortcut function to set the schedule to trigger in January.
pub fn on_januaries(self: DateScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in February.
pub fn on_februaries(self: DateScheduleBuilder) {
  on_month(self, 2)
}

/// Shortcut function to set the schedule to trigger in March.
pub fn on_marches(self: DateScheduleBuilder) {
  on_month(self, 3)
}

/// Shortcut function to set the schedule to trigger in April.
pub fn on_aprils(self: DateScheduleBuilder) {
  on_month(self, 4)
}

/// Shortcut function to set the schedule to trigger in May.
pub fn on_mays(self: DateScheduleBuilder) {
  on_month(self, 5)
}

/// Shortcut function to set the schedule to trigger in June.
pub fn on_junes(self: DateScheduleBuilder) {
  on_month(self, 6)
}

/// Shortcut function to set the schedule to trigger in July.
pub fn on_julies(self: DateScheduleBuilder) {
  on_month(self, 7)
}

/// Shortcut function to set the schedule to trigger in August.
pub fn on_augusts(self: DateScheduleBuilder) {
  on_month(self, 8)
}

/// Shortcut function to set the schedule to trigger in September.
pub fn on_septembers(self: DateScheduleBuilder) {
  on_month(self, 9)
}

/// Shortcut function to set the schedule to trigger in October.
pub fn on_octobers(self: DateScheduleBuilder) {
  on_month(self, 10)
}

/// Shortcut function to set the schedule to trigger in November.
pub fn on_novembers(self: DateScheduleBuilder) {
  on_month(self, 11)
}

/// Shortcut function to set the schedule to trigger in December.
pub fn on_decembers(self: DateScheduleBuilder) {
  on_month(self, 12)
}

// DAY OF WEEK
//---------------

/// Adds a specific day of the week on which the schedule should trigger.
/// If other specific days are already set, appends the given day.
pub fn on_day_of_week(
  self: DateScheduleBuilder,
  value: Int,
) -> DateScheduleBuilder {
  let on_day_of_week = case self.day_of_week {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    on_day_of_week,
  )
}

/// Sets a range of days of the week during which the schedule should trigger.
pub fn between_day_of_weeks(
  self: DateScheduleBuilder,
  start: Int,
  end: Int,
) -> DateScheduleBuilder {
  let on_day_of_week = case self.day_of_week {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  DateScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    on_day_of_week,
  )
}

/// Shortcut function to set the schedule to trigger on Mondays.
pub fn on_mondays(self: DateScheduleBuilder) {
  on_day_of_week(self, 1)
}

/// Shortcut function to set the schedule to trigger on Thuesdays.
pub fn on_thuesdays(self: DateScheduleBuilder) {
  on_day_of_week(self, 2)
}

/// Shortcut function to set the schedule to trigger on Wednesdays.
pub fn on_wednesdays(self: DateScheduleBuilder) {
  on_day_of_week(self, 3)
}

/// Shortcut function to set the schedule to trigger on Thursdays.
pub fn on_thursdays(self: DateScheduleBuilder) {
  on_day_of_week(self, 4)
}

/// Shortcut function to set the schedule to trigger on Fridays.
pub fn on_fridays(self: DateScheduleBuilder) {
  on_day_of_week(self, 5)
}

/// Shortcut function to set the schedule to trigger on Saturdays.
pub fn on_saturdays(self: DateScheduleBuilder) {
  on_day_of_week(self, 6)
}

/// Shortcut function to set the schedule to trigger on Sundays.
pub fn on_sundays(self: DateScheduleBuilder) {
  on_day_of_week(self, 7)
}

/// Shortcut function to set the schedule to trigger on weekdays (Monday through Friday).
pub fn on_weekdays(self: DateScheduleBuilder) {
  self
  |> on_mondays
  |> on_thuesdays
  |> on_wednesdays
  |> on_thursdays
  |> on_fridays
}

/// Shortcut function to set the schedule to trigger on weekends (Saturday and Sunday).
pub fn on_weekends(self: DateScheduleBuilder) {
  self |> on_saturdays |> on_sundays
}

/// Validate and create Schedule from scheduleBuilder 
pub fn build_schedule(
  schedule: DateScheduleBuilder,
) -> Result(Schedule, DateError) {
  // Validate each second constraint 
  use second <- result.try(
    time_value_in_range(schedule.second, within_range(_, "Seconds", 0, 60)),
  )
  // Validate each minute constraint 
  use minute <- result.try(
    time_value_in_range(schedule.minute, within_range(_, "Minutes", 0, 60)),
  )
  // Validate each hour constraint 
  use hour <- result.try(
    time_value_in_range(schedule.hour, within_range(_, "Hours", 0, 24)),
  )

  // Validate each month constraint 
  use month <- result.try(
    time_value_in_range(schedule.month, within_range(_, "Month", 1, 12)),
  )

  let has_february = value_exists_in_time_value(2, month)

  let has_30_month =
    [
      value_exists_in_time_value(4, month),
      value_exists_in_time_value(6, month),
      value_exists_in_time_value(9, month),
      value_exists_in_time_value(11, month),
    ]
    |> list.any(function.identity)

  let max_day_of_month = case has_february, has_30_month {
    True, _ -> 28
    _, True -> 30
    _, _ -> 31
  }

  // Validate each day_of_month constraint 
  use day_of_month <- result.try(
    time_value_in_range(schedule.day_of_month, within_range(
      _,
      "Day of month",
      1,
      max_day_of_month,
    )),
  )
  // Validate each day_of_week constraint 
  use day_of_week <- result.map(
    time_value_in_range(schedule.day_of_week, within_range(
      _,
      "Day of week",
      1,
      7,
    )),
  )

  DateSchedule(second:, minute:, hour:, day_of_month:, month:, day_of_week:)
  |> Schedule
}

// Builder
//---------------
pub fn new(worker: jobs.Worker, schedule: Schedule) {
  Spec(
    schedule,
    worker,
    max_retries: 3,
    init_timeout: 1000,
    poll_interval: 10_000,
    event_listeners: [],
  )
}

/// Maximum times a job will be retried before being considered failed
/// and moved to the failed_jobs table
///
pub fn with_max_retries(spec: Spec, max_retries: Int) {
  Spec(..spec, max_retries:)
}

/// Amount of time in milli seconds the queue is given to start
///
pub fn with_init_timeout(spec: Spec, init_timeout: Int) {
  Spec(..spec, init_timeout:)
}

/// Time in milli seconds to wait in between checking for new jobs
///
pub fn with_poll_interval_ms(spec: Spec, poll_interval: Int) {
  Spec(..spec, poll_interval:)
}

/// Set event listeners for the queue
/// NOTE: This will override previously added workers
///
pub fn with_event_listeners(
  spec: Spec,
  event_listeners: List(events.EventListener),
) {
  Spec(..spec, event_listeners:)
}

pub fn build(
  registry registry: registries.ScheduledJobRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  spec spec: Spec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      monitor.register_scheduled_job(self, spec.worker.job_name)

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(spec.worker.job_name),
      )

      // Schedule first job
      process.send(self, messages.ScheduleNext)
      // Start polling directly
      process.send(self, messages.StartPolling)

      let state =
        State(
          is_polling: False,
          self:,
          name: spec.worker.job_name,
          schedule: spec.schedule,
          db_adapter:,
          poll_interval: spec.poll_interval,
          max_retries: spec.max_retries,
          send_event: events.send_event(spec.event_listeners, _),
          worker: spec.worker,
        )

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: spec.init_timeout,
    loop:,
  ))
}

// Actor 
//---------------
type State {
  State(
    // state
    is_polling: Bool,
    // settings
    self: process.Subject(Message),
    name: String,
    schedule: Schedule,
    db_adapter: db_adapter.DbAdapter,
    poll_interval: Int,
    max_retries: Int,
    send_event: fn(events.Event) -> Nil,
    worker: jobs.Worker,
  )
}

fn loop(message: Message, state: State) -> actor.Next(Message, State) {
  case message {
    messages.Shutdown -> actor.Stop(process.Normal)
    messages.StartPolling -> {
      process.send_after(
        state.self,
        state.poll_interval,
        messages.WaitBetweenPoll,
      )
      state.send_event(events.QueuePollingStartedEvent(state.name))
      actor.continue(State(..state, is_polling: True))
    }

    messages.StopPolling -> {
      actor.continue(State(..state, is_polling: False))
    }

    messages.HandleError(job, exception) -> {
      // Should always be possible to move job to succeeded otherwise crash
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(events.JobFailedEvent(state.name, job))
      actor.send(state.self, messages.ScheduleNext)
      actor.continue(state)
    }

    messages.HandleSuccess(job) -> {
      // Should always be possible to move job to succeeded otherwise crash
      let assert Ok(_) = state.db_adapter.move_job_to_succeeded(job)
      state.send_event(events.JobSucceededEvent(state.name, job))
      actor.send(state.self, messages.ScheduleNext)
      actor.continue(state)
    }

    messages.ScheduleNext -> {
      case state.db_adapter.get_enqueued_jobs(state.worker.job_name) {
        Ok([]) -> {
          let next_run_date = get_next_run_date(state)
          // No jobs enqueued; attempt to schedule the next run
          let assert Ok(_) = schedule_next(state, next_run_date)
          Nil
        }
        // Do nothing if there is one scheduled job
        Ok([_one]) -> Nil
        // Cleanup job overflow
        Ok(more) -> {
          more
          |> list.each(state.db_adapter.move_job_to_failed(
            _,
            "Too many scheduled jobs",
          ))
          state.send_event(events.QueueErrorEvent(
            state.name,
            errors.ScheduleError("Too many queued jobs, cleaning up"),
          ))
          Nil
        }
        Error(_db_error) -> {
          // Error is logged in db_adapter
          Nil
        }
      }

      actor.send(state.self, messages.WaitBetweenPoll)
      actor.continue(state)
    }

    messages.WaitBetweenPoll -> {
      case state.is_polling {
        True -> {
          process.send_after(
            state.self,
            state.poll_interval,
            messages.ProcessJobs,
          )
          actor.continue(state)
        }
        False -> {
          actor.continue(state)
        }
      }
    }

    messages.ProcessJobs -> {
      let jobs = case
        state.db_adapter.reserve_jobs(
          [state.worker.job_name],
          1,
          utils.pid_to_string(process.subject_owner(state.self)),
        )
      {
        Ok(jobs) -> {
          jobs
        }
        Error(err) -> {
          state.send_event(events.QueueErrorEvent(state.name, err))
          panic as "Job store paniced"
        }
      }

      list.each(jobs, fn(job) {
        state.send_event(events.JobReservedEvent(state.name, job))
        execute_scheduled_job(job, state.worker, state)
      })

      actor.send(state.self, messages.ScheduleNext)
      actor.continue(state)
    }
  }
}

fn get_next_run_date(state: State) {
  case state.schedule {
    Interval(interval) -> {
      naive_datetime.now_utc()
      |> naive_datetime.add(to_gtempo(interval))
      |> naive_datetime.to_tuple()
    }

    Schedule(schedule) -> {
      schedule
      |> next_run_date(naive_datetime.now_utc(), _)
    }
  }
}

fn schedule_next(
  state: State,
  next_available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
) {
  let job =
    state.db_adapter.enqueue_job(state.worker.job_name, "", next_available_at)

  case job {
    Ok(job) -> {
      state.send_event(events.JobEnqueuedEvent(job))
      Ok(job)
    }
    Error(e) -> {
      state.send_event(events.QueueErrorEvent(state.worker.job_name, e))
      Error(e)
    }
  }
}

fn execute_scheduled_job(job: jobs.Job, worker: jobs.Worker, state: State) {
  state.send_event(events.JobStartEvent(state.name, job))
  case worker.handler(job), job.attempts < state.max_retries {
    Ok(_), _ -> {
      actor.send(state.self, messages.HandleSuccess(job))
    }
    Error(_), True -> {
      let assert Ok(new_job) = state.db_adapter.increment_attempts(job)
      execute_scheduled_job(new_job, worker, state)
    }
    Error(err), False -> {
      actor.send(state.self, messages.HandleError(job, err))
    }
  }
}

// Get next run date
//---------------

@internal
pub fn next_run_date(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> #(#(Int, Int, Int), #(Int, Int, Int)) {
  next_run_loop(date, date, schedule)
}

fn next_run_loop(
  date: tempo.NaiveDateTime,
  start_date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> #(#(Int, Int, Int), #(Int, Int, Int)) {
  let new_date =
    date
    |> align_seconds(schedule)
    |> align_minutes(schedule)
    |> align_hours(schedule)
    |> align_day(schedule)
    |> align_month(schedule)

  case naive_datetime.compare(new_date, start_date) {
    // If date is same as start date, increase the seconds by one and 
    // try again to get the next run date
    order.Eq ->
      next_run_loop(
        naive_datetime.add(date, duration.seconds(1)),
        start_date,
        schedule,
      )
    order.Gt -> new_date |> naive_datetime.to_tuple
    order.Lt -> panic as "Next run date is in the past"
  }
}

@internal
pub fn align_seconds(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> tempo.NaiveDateTime {
  let second = naive_datetime.get_time(date) |> time.get_second()

  case is_matching(second, schedule.second) {
    True -> date
    False -> {
      let next_date = naive_datetime.add(date, duration.seconds(1))
      align_seconds(next_date, schedule)
    }
  }
}

@internal
pub fn align_minutes(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> tempo.NaiveDateTime {
  let minute = naive_datetime.get_time(date) |> time.get_minute()

  case is_matching(minute, schedule.minute) {
    True -> date
    False -> {
      let next_date = naive_datetime.add(date, duration.minutes(1))
      align_minutes(next_date, schedule)
    }
  }
}

@internal
pub fn align_hours(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> tempo.NaiveDateTime {
  let hour = naive_datetime.get_time(date) |> time.get_hour()

  case is_matching(hour, schedule.hour) {
    True -> date
    False -> {
      let next_date = naive_datetime.add(date, duration.hours(1))
      align_hours(next_date, schedule)
    }
  }
}

@internal
pub fn align_day(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> tempo.NaiveDateTime {
  let day_of_month = naive_datetime.get_date(date) |> date.get_day()
  let day_of_week =
    naive_datetime.get_date(date) |> date.to_day_of_week_number()

  // Match on either day of week, day of month or if both are Every
  let match = case schedule.day_of_week, schedule.day_of_month {
    Every, Every -> True
    Specific(_), Specific(_) ->
      [
        is_matching(day_of_month, schedule.day_of_month),
        is_matching(day_of_week, schedule.day_of_week),
      ]
      |> list.find(fn(v) { v })
      |> result.is_ok
    Every, Specific(_) -> is_matching(day_of_month, schedule.day_of_month)
    Specific(_), Every -> is_matching(day_of_week, schedule.day_of_week)
  }

  case match {
    True -> date
    False -> {
      let next_date = naive_datetime.add(date, duration.days(1))
      align_day(next_date, schedule)
    }
  }
}

@internal
pub fn align_month(
  date: tempo.NaiveDateTime,
  schedule: DateSchedule,
) -> tempo.NaiveDateTime {
  let year = naive_datetime.get_date(date) |> date.get_year()
  let month = naive_datetime.get_date(date) |> date.get_month()
  let month_number = month |> month.to_int()

  case is_matching(month_number, schedule.month) {
    True -> date
    False -> {
      let days = month.days(of: month, in: year)
      let next_date = naive_datetime.add(date, duration.days(days))
      align_month(next_date, schedule)
    }
  }
}

// Get next run date utils
//---------------

fn is_matching(unit: Int, schedule: TimeValue) -> Bool {
  case schedule {
    Every -> Ok(Nil)
    Specific(l) -> {
      list.find(l, fn(i) {
        case i {
          Range(start, end) -> unit > start && unit < end
          Value(val) -> unit == val
        }
      })
      |> result.replace(Nil)
    }
  }
  |> result.is_ok
}

/// Validate a specific value is within a range
fn within_range(
  v: Int,
  name: String,
  min: Int,
  max: Int,
) -> Result(Int, DateError) {
  case min <= v && v <= max {
    True -> Ok(v)
    False ->
      Error(OutOfBoundsError(
        name
        <> " must be between "
        <> int.to_string(min)
        <> " and "
        <> int.to_string(max)
        <> ", found: "
        <> int.to_string(v),
      ))
  }
}

/// Validate all entries in a TimeValue is within a range
fn time_value_in_range(
  time_value: TimeValue,
  validator: fn(Int) -> Result(Int, DateError),
) -> Result(TimeValue, DateError) {
  case time_value {
    Every -> Ok(Every)
    Specific(list) -> {
      list.map(list, fn(d) {
        case d {
          Value(v) -> {
            validator(v)
            |> result.map(Value)
          }
          Range(start, end) -> {
            validator(start)
            |> result.try(fn(start) {
              use end <- result.map(validator(end))
              Range(start, end)
            })
          }
        }
      })
      |> result.all()
      |> result.map(Specific)
    }
  }
}

/// Check if a value exists in a TimeValue (value or range)
fn value_exists_in_time_value(value: Int, time_value: TimeValue) -> Bool {
  case time_value {
    Every -> False
    // Value always exists in "Every"
    Specific(list) -> {
      list.any(list, fn(d) {
        case d {
          Value(v) -> v == value
          // Direct match
          Range(start, end) -> start <= value && value <= end
          // Range check
        }
      })
    }
  }
}
