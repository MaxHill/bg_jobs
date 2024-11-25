import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/bg_jobs_ffi
import bg_jobs/internal/dispatcher
import bg_jobs/internal/dispatcher_messages
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages.{type Message} as messages
import bg_jobs/internal/utils
import bg_jobs/jobs
import birl
import birl/duration as birl_duration
import chip
import gleam/erlang/process
import gleam/function
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/string

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
  Schedule(ScheduleBuilder)
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
  Month(Int)
  Year(Int)
}

@internal
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

/// Create interval schedule in months
pub fn new_interval_months(months: Int) {
  Interval(Month(months))
}

/// Create interval schedule in years
pub fn new_interval_years(years: Int) {
  Interval(Year(years))
}

// Schdedule
//---------------
pub type ScheduleBuilder {
  ScheduleBuilder(
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

/// Create a new schedule that runs on the first second
/// every minut
pub fn new_schedule() -> ScheduleBuilder {
  ScheduleBuilder(
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
pub fn every_second(self: ScheduleBuilder) -> ScheduleBuilder {
  ScheduleBuilder(
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
pub fn on_second(self: ScheduleBuilder, second: Int) -> ScheduleBuilder {
  let second = case self.second {
    Every -> Specific([Value(second)])
    Specific(values) -> Specific(list.flatten([values, [Value(second)]]))
  }

  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let second = case self.second {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
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
pub fn on_minute(self: ScheduleBuilder, value: Int) -> ScheduleBuilder {
  let minute = case self.minute {
    Every -> {
      Specific([Value(value)])
    }
    Specific(values) -> {
      Specific(list.flatten([values, [Value(value)]]))
    }
  }
  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let minute = case self.minute {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
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
pub fn on_hour(self: ScheduleBuilder, value: Int) -> ScheduleBuilder {
  let hour = case self.hour {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let hour = case self.hour {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
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
pub fn on_day_of_month(self: ScheduleBuilder, value: Int) -> ScheduleBuilder {
  let day_of_month = case self.day_of_month {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let day_of_month = case self.day_of_month {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
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
pub fn on_month(self: ScheduleBuilder, value: Int) -> ScheduleBuilder {
  let month = case self.month {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let month = case self.month {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    month,
    self.day_of_week,
  )
}

/// Shortcut function to set the schedule to trigger in January.
pub fn on_januaries(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in February.
pub fn on_februaries(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in March.
pub fn on_marches(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in April.
pub fn on_aprils(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in May.
pub fn on_mays(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in June.
pub fn on_junes(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in July.
pub fn on_julies(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in August.
pub fn on_augusts(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in September.
pub fn on_septembers(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in October.
pub fn on_octobers(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in November.
pub fn on_novembers(self: ScheduleBuilder) {
  on_month(self, 1)
}

/// Shortcut function to set the schedule to trigger in December.
pub fn on_decembers(self: ScheduleBuilder) {
  on_month(self, 1)
}

// DAY OF WEEK
//---------------

/// Adds a specific day of the week on which the schedule should trigger.
/// If other specific days are already set, appends the given day.
pub fn on_day_of_week(self: ScheduleBuilder, value: Int) -> ScheduleBuilder {
  let on_day_of_week = case self.day_of_week {
    Every -> Specific([Value(value)])
    Specific(values) -> Specific(list.flatten([values, [Value(value)]]))
  }
  ScheduleBuilder(
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
  self: ScheduleBuilder,
  start: Int,
  end: Int,
) -> ScheduleBuilder {
  let on_day_of_week = case self.day_of_week {
    Every -> Specific([Range(start, end)])
    Specific(values) -> Specific(list.flatten([values, [Range(start, end)]]))
  }
  ScheduleBuilder(
    self.second,
    self.minute,
    self.hour,
    self.day_of_month,
    self.month,
    on_day_of_week,
  )
}

/// Shortcut function to set the schedule to trigger on Mondays.
pub fn on_mondays(self: ScheduleBuilder) {
  on_day_of_week(self, 1)
}

/// Shortcut function to set the schedule to trigger on Thuesdays.
pub fn on_thuesdays(self: ScheduleBuilder) {
  on_day_of_week(self, 2)
}

/// Shortcut function to set the schedule to trigger on Wednesdays.
pub fn on_wednesdays(self: ScheduleBuilder) {
  on_day_of_week(self, 3)
}

/// Shortcut function to set the schedule to trigger on Thursdays.
pub fn on_thursdays(self: ScheduleBuilder) {
  on_day_of_week(self, 4)
}

/// Shortcut function to set the schedule to trigger on Fridays.
pub fn on_fridays(self: ScheduleBuilder) {
  on_day_of_week(self, 5)
}

/// Shortcut function to set the schedule to trigger on Saturdays.
pub fn on_saturdays(self: ScheduleBuilder) {
  on_day_of_week(self, 6)
}

/// Shortcut function to set the schedule to trigger on Sundays.
pub fn on_sundays(self: ScheduleBuilder) {
  on_day_of_week(self, 7)
}

/// Shortcut function to set the schedule to trigger on weekdays (Monday through Friday).
pub fn on_weekdays(self: ScheduleBuilder) {
  self
  |> on_mondays
  |> on_thuesdays
  |> on_wednesdays
  |> on_thursdays
  |> on_fridays
}

/// Shortcut function to set the schedule to trigger on weekends (Saturday and Sunday).
pub fn on_weekends(self: ScheduleBuilder) {
  self |> on_saturdays |> on_sundays
}

/// Convert the ScheduleBuilder to actual cron syntax
pub fn to_cron_syntax(self: ScheduleBuilder) -> String {
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

/// Create Schedule from scheduleBuilder
pub fn build_schedule(schedule: ScheduleBuilder) {
  schedule
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
  dispatch_registry dispatch_registry: registries.DispatcherRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  spec spec: Spec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Cleanup previously in flight jobs by this queue name
      let assert Ok(_) =
        utils.remove_in_flight_jobs(spec.worker.job_name, db_adapter)

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
          dispatch_registry: dispatch_registry,
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
    dispatch_registry: registries.DispatcherRegistry,
  )
}

fn loop(message: Message, state: State) -> actor.Next(Message, State) {
  case message {
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
        state.db_adapter.claim_jobs([state.worker.job_name], 1, state.name)
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
      birl.now()
      |> birl.add(to_birl(interval))
      |> birl.to_erlang_datetime()
    }

    Schedule(schedule) -> {
      schedule
      |> to_cron_syntax()
      |> bg_jobs_ffi.get_next_run_date(birl.now() |> birl.to_erlang_datetime())
    }
  }
}

fn schedule_next(
  state: State,
  next_available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
) {
  // Job dispatcher should always exist otherwise crash
  let assert Ok(queue) = chip.find(state.dispatch_registry, dispatcher.name)

  // Errors are handled in the dispatcher 
  let _scheduled_job =
    process.try_call(
      queue,
      dispatcher_messages.EnqueueJob(
        _,
        state.worker.job_name,
        "",
        next_available_at,
      ),
      1000,
    )
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
