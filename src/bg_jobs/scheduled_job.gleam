import bg_jobs/db_adapter
import bg_jobs/internal/errors
import bg_jobs/internal/events
import bg_jobs/internal/jobs
import bg_jobs/internal/messages as other_messages
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages.{type Message} as messages
import bg_jobs/internal/time
import bg_jobs/internal/utils
import bg_jobs/internal/worker
import bg_jobs_ffi
import birl
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/otp/actor

pub type Spec {
  Spec(
    schedule: Schedule,
    worker: worker.Worker,
    max_retries: Int,
    init_timeout: Int,
    poll_interval: Int,
    event_listeners: List(events.EventListener),
  )
}

pub type Schedule {
  Interval(time.Duration)
  Advanced(time.CronSchedule)
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

pub fn create(
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
pub type State {
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
    worker: worker.Worker,
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
      let next_run_date = get_next_run_date(state)

      case state.db_adapter.get_enqueued_jobs(state.worker.job_name) {
        Ok([]) -> {
          // No jobs enqueued; attempt to schedule the next run
          let assert Ok(_) = schedule_next(state, next_run_date)
          Nil
        }
        // Do nothing if there is one scheduled job
        Ok([_one]) -> Nil
        Ok(_more) -> {
          state.send_event(events.QueueErrorEvent(
            state.name,
            errors.ScheduleError("Too many queued jobs, crashing"),
          ))
          // If there are more than one scheduled something is wrong
          panic as "There are too many queued jobs"
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

    Advanced(schedule) -> {
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
  let assert Ok(queue) = chip.find(state.dispatch_registry, "job_dispatcher")

  // Errors are handled in the dispatcher 
  let _scheduled_job =
    process.try_call(
      queue,
      other_messages.EnqueueJob(_, state.worker.job_name, "", next_available_at),
      1000,
    )
}

fn execute_scheduled_job(job: jobs.Job, worker: worker.Worker, state: State) {
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
