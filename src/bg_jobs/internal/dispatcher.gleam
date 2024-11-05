import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/internal/dispatcher_messages as messages
import bg_jobs/internal/events
import bg_jobs/internal/registries
import bg_jobs/jobs
import birl
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option
import gleam/otp/actor
import youid/uuid

import chip

pub const name = "job_dispatcher"

pub fn build(
  registry registry: registries.DispatcherRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  workers workers: List(jobs.Worker),
  event_listners event_listeners: List(events.EventListener),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(name),
      )

      let state =
        DispatcherState(
          db_adapter: db_adapter,
          workers: workers,
          send_event: events.send_event(event_listeners, _),
          self:,
        )

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: 10_000,
    loop: dispatcher_loop,
  ))
}

// Dispatcher Actor 
//---------------

pub type DispatcherState {
  DispatcherState(
    self: process.Subject(messages.Message),
    db_adapter: db_adapter.DbAdapter,
    send_event: fn(events.Event) -> Nil,
    workers: List(jobs.Worker),
  )
}

pub fn dispatcher_loop(
  message: messages.Message,
  state: DispatcherState,
) -> actor.Next(messages.Message, DispatcherState) {
  case message {
    messages.HandleDispatchError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(events.JobFailedEvent(name, job))
      actor.continue(state)
    }
    messages.EnqueueJob(client, job_name, payload, available_at) -> {
      let has_worker_for_job_name =
        list.map(state.workers, fn(job) { job.job_name })
        |> list.find(fn(name) { name == job_name })

      case has_worker_for_job_name {
        Ok(name) -> {
          let job = state.db_adapter.enqueue_job(name, payload, available_at)
          process.send(client, job)
          case job {
            Ok(job) -> state.send_event(events.JobEnqueuedEvent(job))
            Error(e) -> state.send_event(events.QueueErrorEvent(name, e))
          }
        }
        Error(_) -> {
          let job =
            jobs.Job(
              id: uuid.v4_string(),
              name: job_name,
              payload: payload,
              attempts: 0,
              created_at: birl.now() |> birl.to_erlang_datetime(),
              available_at: birl.to_erlang_datetime(
                birl.from_erlang_universal_datetime(available_at),
              ),
              reserved_at: option.None,
              reserved_by: option.None,
            )
          actor.send(
            state.self,
            messages.HandleDispatchError(
              job,
              "Could not enqueue job with no worker",
            ),
          )

          let error = errors.DispatchJobError("No worker for job: " <> job_name)
          process.send(client, Error(error))
          state.send_event(events.QueueErrorEvent(name, error))
        }
      }
      actor.continue(state)
    }
  }
}
