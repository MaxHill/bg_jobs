import bg_jobs/db_adapter
import bg_jobs/events
import bg_jobs/internal/monitor
import bg_jobs/internal/queue_messages as messages
import bg_jobs/internal/registries
import bg_jobs/internal/utils
import bg_jobs/jobs
import chip
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result

/// Represents a queue that should be created
///
pub type Spec {
  Spec(
    name: String,
    workers: List(jobs.Worker),
    max_retries: Int,
    init_timeout: Int,
    max_concurrent_jobs: Int,
    poll_interval: Int,
    event_listeners: List(events.EventListener),
  )
}

/// Start building a new queue-spec 
///
/// NOTE: Name needs to be the same on restarts, but unique across all
/// queues, even if running on different machines
///
pub fn new(name: String) {
  Spec(
    name: name,
    max_retries: 3,
    init_timeout: 1000,
    max_concurrent_jobs: 10,
    poll_interval: 100,
    workers: [],
    event_listeners: [],
  )
}

/// Set the name of the queue
///
/// NOTE: This needs to be the same on restarts, but unique across all
/// queues, even if running on different machines
///
pub fn with_name(spec: Spec, name: String) {
  Spec(..spec, name:)
}

/// Maximum times a job will be retried before being considered failed
/// and moved to the failed_jobs table
///
pub fn with_max_retries(spec: Spec, max_retries: Int) {
  Spec(..spec, max_retries:)
}

/// Amount of time in milliseconds the queue is given to start
///
pub fn with_init_timeout(spec: Spec, init_timeout: Int) {
  Spec(..spec, init_timeout:)
}

/// Maximum number of jobs to run in parallel
///
pub fn with_max_concurrent_jobs(spec: Spec, max_concurrent_jobs: Int) {
  Spec(..spec, max_concurrent_jobs:)
}

/// Time in milli seconds to wait in between checking for new jobs
///
pub fn with_poll_interval_ms(spec: Spec, poll_interval: Int) {
  Spec(..spec, poll_interval:)
}

/// Add multiple workers the queue will poll for and run
///
pub fn with_workers(spec: Spec, workers: List(jobs.Worker)) {
  Spec(..spec, workers: list.flatten([spec.workers, workers]))
}

/// Add a worker the queue will poll for and run
///
pub fn with_worker(spec: Spec, worker: jobs.Worker) {
  Spec(..spec, workers: list.flatten([spec.workers, [worker]]))
}

/// Append a list of event listeners for the queue
///
pub fn with_event_listeners(
  spec: Spec,
  event_listeners: List(events.EventListener),
) {
  Spec(
    ..spec,
    event_listeners: list.flatten([spec.event_listeners, event_listeners]),
  )
}

/// Add a event listener to the list of event listners for the queue
///
pub fn with_event_listener(spec: Spec, event_listener: events.EventListener) {
  Spec(
    ..spec,
    event_listeners: list.flatten([spec.event_listeners, [event_listener]]),
  )
}

/// Create the actual queue actor
///
/// NOTE: This will, clear all in flight jobs for this queue name
/// therefore it's important to have a unique name across all queues, 
/// even if running on different machine
///
@internal
pub fn build(
  monitor_registry monitor_registry: registries.MonitorRegistry,
  registry registry: registries.QueueRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  spec spec: Spec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      let assert Ok(monitor_subject) = chip.find(monitor_registry, monitor.name)
      monitor.register(monitor_subject, self)
      // Cleanup previously in flight jobs by this queue name
      // let assert Ok(_) = utils.remove_in_flight_jobs(spec.name, db_adapter)

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(spec.name),
      )

      // Start polling directly
      process.send(self, messages.StartPolling)

      let state =
        QueueState(
          name: spec.name,
          db_adapter: db_adapter,
          workers: spec.workers,
          send_event: events.send_event(spec.event_listeners, _),
          poll_interval: spec.poll_interval,
          is_polling: False,
          max_retries: spec.max_retries,
          max_concurrent_jobs: spec.max_concurrent_jobs,
          active_jobs: 0,
          self:,
        )

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: spec.init_timeout,
    loop: loop,
  ))
}

// QueueActor 
//---------------

type QueueState {
  QueueState(
    // state
    is_polling: Bool,
    active_jobs: Int,
    // settings
    self: process.Subject(messages.Message),
    name: String,
    db_adapter: db_adapter.DbAdapter,
    poll_interval: Int,
    max_retries: Int,
    max_concurrent_jobs: Int,
    send_event: fn(events.Event) -> Nil,
    workers: List(jobs.Worker),
  )
}

fn loop(
  message: messages.Message,
  state: QueueState,
) -> actor.Next(messages.Message, QueueState) {
  case message {
    messages.Shutdown -> actor.Stop(process.Normal)
    messages.StartPolling -> {
      process.send_after(
        state.self,
        state.poll_interval,
        messages.WaitBetweenPoll,
      )
      state.send_event(events.QueuePollingStartedEvent(state.name))
      actor.continue(QueueState(..state, is_polling: True))
    }

    messages.StopPolling -> {
      actor.continue(QueueState(..state, is_polling: False))
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
        False -> actor.continue(state)
      }
    }

    messages.HandleError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(events.JobFailedEvent(state.name, job))
      actor.continue(QueueState(..state, active_jobs: state.active_jobs - 1))
    }

    messages.HandleSuccess(job) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_succeeded(job)
      state.send_event(events.JobSucceededEvent(state.name, job))
      actor.continue(QueueState(..state, active_jobs: state.active_jobs - 1))
    }

    messages.ProcessJobs -> {
      let new_jobs_limit = state.max_concurrent_jobs - state.active_jobs
      case new_jobs_limit {
        0 -> {
          actor.send(state.self, messages.WaitBetweenPoll)
          actor.continue(state)
        }
        _ -> {
          let jobs = case
            state.db_adapter.reserve_jobs(
              list.map(state.workers, fn(job) { job.job_name }),
              new_jobs_limit,
              utils.pid_to_string(process.subject_owner(state.self)),
            )
          {
            Ok(jobs) -> jobs
            Error(err) -> {
              state.send_event(events.QueueErrorEvent(state.name, err))
              panic as "jobs.Job store paniced"
            }
          }

          list.each(jobs, fn(job) {
            state.send_event(events.JobReservedEvent(state.name, job))
            case match_worker(state.workers, job) {
              option.Some(worker) -> {
                run_worker(job, worker, state)
                Nil
              }
              option.None -> {
                let assert Ok(_) =
                  state.db_adapter.move_job_to_failed(
                    job,
                    "Could not find worker for job",
                  )
                Nil
              }
            }
          })

          let active_jobs = state.active_jobs + list.length(jobs)
          actor.send(state.self, messages.WaitBetweenPoll)
          actor.continue(QueueState(..state, active_jobs:))
        }
      }
    }
  }
}

fn match_worker(workers: List(jobs.Worker), job: jobs.Job) {
  workers
  |> list.map(fn(worker) {
    case worker.job_name == job.name {
      True -> Ok(worker)
      False -> Error(Nil)
    }
  })
  |> result.values
  |> list.first
  |> option.from_result
}

fn run_worker(job: jobs.Job, worker: jobs.Worker, state: QueueState) {
  process.start(
    fn() {
      state.send_event(events.JobStartEvent(state.name, job))
      case worker.handler(job), job.attempts < state.max_retries {
        Ok(_), _ -> {
          actor.send(state.self, messages.HandleSuccess(job))
        }
        Error(_), True -> {
          let assert Ok(new_job) = state.db_adapter.increment_attempts(job)
          run_worker(new_job, worker, state)
        }
        Error(err), False -> {
          actor.send(state.self, messages.HandleError(job, err))
        }
      }
    },
    False,
  )
  Nil
}
