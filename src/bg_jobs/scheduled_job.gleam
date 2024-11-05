import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/internal/dispatcher
import bg_jobs/internal/dispatcher_messages
import bg_jobs/internal/events
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages.{type Message} as messages
import bg_jobs/internal/time
import bg_jobs/internal/utils
import bg_jobs/jobs
import bg_jobs_ffi
import birl
import chip
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/otp/actor

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
  Interval(time.Duration)
  Schedule(time.CronSchedule)
}

// Reexport
pub const new_schedule = time.new_schedule

pub const every_second = time.every_second

pub const on_second = time.on_second

pub const on_seconds = time.on_seconds

pub const between_seconds = time.between_seconds

pub const every_minute = time.every_minute

pub const on_minute = time.on_minute

pub const on_minutes = time.on_minutes

pub const between_minutes = time.between_minutes

pub const every_hour = time.every_hour

pub const hour = time.hour

pub const day_of_month = time.day_of_month

pub const month = time.month

pub const day_of_week = time.day_of_week

pub const to_cron_syntax = time.to_cron_syntax

pub fn interval_milliseconds(milliseconds: Int) {
  Interval(time.Millisecond(milliseconds))
}

pub fn interval_seconds(seconds: Int) {
  Interval(time.Second(seconds))
}

pub fn interval_minutes(minutes: Int) {
  Interval(time.Minute(minutes))
}

pub fn interval_hours(hours: Int) {
  Interval(time.Hour(hours))
}

pub fn interval_days(days: Int) {
  Interval(time.Day(days))
}

pub fn interval_weeks(weeks: Int) {
  Interval(time.Week(weeks))
}

pub fn interval_months(months: Int) {
  Interval(time.Month(months))
}

pub fn interval_years(years: Int) {
  Interval(time.Year(years))
}

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

// Scheduled job Actor 
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
      // Should always be possible to move job to succeded otherwise crash
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(events.JobFailedEvent(state.name, job))
      actor.send(state.self, messages.ScheduleNext)
      actor.continue(state)
    }

    messages.HandleSuccess(job) -> {
      // Should always be possible to move job to succeded otherwise crash
      let assert Ok(_) = state.db_adapter.move_job_to_succeded(job)
      state.send_event(events.JobSuccededEvent(state.name, job))
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
      |> birl.add(time.to_birl(interval))
      |> birl.to_erlang_datetime()
    }

    Schedule(schedule) -> {
      schedule
      |> time.to_cron_syntax()
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
