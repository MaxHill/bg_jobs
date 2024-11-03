import bg_jobs/db_adapter
import bg_jobs/internal/errors
import bg_jobs/internal/events
import bg_jobs/internal/jobs
import bg_jobs/internal/messages
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages
import bg_jobs/internal/utils
import bg_jobs/internal/worker
import bg_jobs/scheduled_job
import birl
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervisor
import gleam/result
import youid/uuid

pub type BgJobs {
  BgJobs(
    supervisor: process.Subject(supervisor.Message),
    queue_registry: registries.QueueRegistry,
    dispatcher_registry: registries.DispatcherRegistry,
    scheduled_jobs_registry: registries.ScheduledJobRegistry,
  )
}

// Queue
//---------------

type ContextBuilder {
  ContextBuilder(
    caller: process.Subject(registries.Registries),
    queue_registry: registries.QueueRegistry,
    dispatcher_registry: option.Option(registries.DispatcherRegistry),
    scheduled_jobs_registry: option.Option(registries.ScheduledJobRegistry),
  )
}

type Context {
  Context(
    caller: process.Subject(registries.Registries),
    queue_registry: registries.QueueRegistry,
    dispatcher_registry: registries.DispatcherRegistry,
    scheduled_jobs_registry: registries.ScheduledJobRegistry,
  )
}

pub type QueueSupervisorSpec {
  QueueSupervisorSpec(
    db_adapter: db_adapter.DbAdapter,
    event_listeners: List(events.EventListener),
    frequency_period: Int,
    max_frequency: Int,
    queues: List(QueueSpec),
    scheduled_jobs: List(scheduled_job.Spec),
  )
}

/// Start building a new spec for bg_jobs.
///
pub fn new(db_adapter: db_adapter.DbAdapter) {
  QueueSupervisorSpec(
    db_adapter: db_adapter,
    event_listeners: [],
    frequency_period: 1,
    max_frequency: 5,
    queues: [],
    scheduled_jobs: [],
  )
}

/// Set the supervisors max frequency
///
pub fn with_supervisor_max_frequency(
  spec: QueueSupervisorSpec,
  max_frequency: Int,
) {
  QueueSupervisorSpec(..spec, max_frequency:)
}

/// Set the supervisors frequency period
///
pub fn with_supervisor_frequency_period(
  spec: QueueSupervisorSpec,
  frequency_period: Int,
) {
  QueueSupervisorSpec(..spec, frequency_period:)
}

/// Add an event_listener to all queues under the supervisor
///
pub fn add_event_listener(
  spec: QueueSupervisorSpec,
  event_listener: events.EventListener,
) {
  QueueSupervisorSpec(
    ..spec,
    event_listeners: list.concat([spec.event_listeners, [event_listener]]),
  )
}

/// Add a queue-spec to create a new queue with the supervisor
///
pub fn add_queue(spec: QueueSupervisorSpec, queue: QueueSpec) {
  QueueSupervisorSpec(..spec, queues: list.concat([spec.queues, [queue]]))
}

/// Add a scheduled job spec to create a new queue with the supervisor
///
pub fn add_scheduled_job(
  spec: QueueSupervisorSpec,
  scheduled_job: scheduled_job.Spec,
) {
  QueueSupervisorSpec(
    ..spec,
    scheduled_jobs: list.concat([spec.scheduled_jobs, [scheduled_job]]),
  )
}

/// Represents a queue that should be created under the queue supervisor
pub type QueueSpec {
  QueueSpec(
    name: String,
    workers: List(worker.Worker),
    max_retries: Int,
    init_timeout: Int,
    max_concurrent_jobs: Int,
    poll_interval: Int,
    event_listeners: List(events.EventListener),
  )
}

/// Start building a new queue-spec that should be created under the queue supervisor
pub fn new_queue(name: String) {
  QueueSpec(
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
/// NOTE: This needs to be reproducable on restarts, but unique across all queues, 
/// even if running on different machines
///
pub fn queue_with_name(spec: QueueSpec, name: String) {
  QueueSpec(..spec, name:)
}

/// Maximum times a job will be retried before being considered failed
/// and moved to the failed_jobs table
///
pub fn queue_with_max_retries(spec: QueueSpec, max_retries: Int) {
  QueueSpec(..spec, max_retries:)
}

/// Amount of time in milli seconds the queue is given to start
///
pub fn queue_with_init_timeout(spec: QueueSpec, init_timeout: Int) {
  QueueSpec(..spec, init_timeout:)
}

/// Maximum number of jobs to run in parallel
///
pub fn queue_with_max_concurrent_jobs(spec: QueueSpec, max_concurrent_jobs: Int) {
  QueueSpec(..spec, max_concurrent_jobs:)
}

/// Time in milli seconds to wait in between checking for new jobs
///
pub fn queue_with_poll_interval_ms(spec: QueueSpec, poll_interval: Int) {
  QueueSpec(..spec, poll_interval:)
}

/// Set workers the queue will poll for and run
/// NOTE: This will override previously added workers
///
pub fn queue_with_workers(spec: QueueSpec, workers: List(worker.Worker)) {
  QueueSpec(..spec, workers:)
}

/// Add a worker to the list the queue will poll for and run
///
pub fn queue_add_worker(spec: QueueSpec, worker: worker.Worker) {
  QueueSpec(..spec, workers: list.concat([spec.workers, [worker]]))
}

/// Set event listeners for the queue
/// NOTE: This will override previously added workers
///
pub fn queue_with_event_listeners(
  spec: QueueSpec,
  event_listeners: List(events.EventListener),
) {
  QueueSpec(..spec, event_listeners:)
}

/// Append a list of event listeners for the queue
///
pub fn queue_add_event_listeners(
  spec: QueueSpec,
  event_listeners: List(events.EventListener),
) {
  QueueSpec(
    ..spec,
    event_listeners: list.concat([spec.event_listeners, event_listeners]),
  )
}

/// Add a event listener to the list of event listners for the queue
///
pub fn queue_add_event_listener(
  spec: QueueSpec,
  event_listener: events.EventListener,
) {
  QueueSpec(
    ..spec,
    event_listeners: list.concat([spec.event_listeners, [event_listener]]),
  )
}

/// Create the actual queue actor
/// NOTE: This will, clear all in flight jobs for this queues name
/// therefore it's important to have a unique name across all queues, 
/// even if running on different machine
///
pub fn create_queue(
  registry registry: registries.QueueRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  queue_spec spec: QueueSpec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Cleanup previously in flight jobs by this queue name
      let assert Ok(_) = utils.remove_in_flight_jobs(spec.name, db_adapter)

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
          queue_name: spec.name,
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
    loop: queue_loop,
  ))
}

pub fn create_dispatcher(
  registry registry: registries.DispatcherRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
  workers workers: List(worker.Worker),
  event_listners event_listeners: List(events.EventListener),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag("job_dispatcher"),
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

/// Create the supervisor and all it's queues based on the provided spec
///
pub fn create(spec: QueueSupervisorSpec) -> Result(BgJobs, errors.BgJobError) {
  let self = process.new_subject()

  let all_workers =
    spec.queues
    |> list.map(fn(spec) { spec.workers })
    |> list.concat
    |> list.append(
      spec.scheduled_jobs
      |> list.map(fn(spec) { spec.worker }),
    )

  let supervisor =
    supervisor.start_spec(
      supervisor.Spec(
        argument: self,
        max_frequency: spec.max_frequency,
        frequency_period: spec.max_frequency,
        init: fn(children) {
          children
          |> supervisor_add_registry_workers()
          // Add the dispatch worker
          |> supervisor.add(create_dispatcher_worker(
            spec.db_adapter,
            all_workers,
            spec.event_listeners,
          ))
          // Add the queues
          |> fn(children) {
            spec.queues
            |> list.map(fn(queue_spec) {
              supervisor.worker(fn(context: Context) {
                queue_spec
                |> queue_with_event_listeners(spec.event_listeners)
                |> create_queue(
                  registry: context.queue_registry,
                  db_adapter: spec.db_adapter,
                  queue_spec: _,
                )
              })
            })
            |> list.fold(children, supervisor.add)
          }
          // Add the scheduled_jobs
          |> fn(children) {
            spec.scheduled_jobs
            |> list.map(fn(scheduled_jobs_spec) {
              supervisor.worker(fn(context: Context) {
                scheduled_jobs_spec
                |> scheduled_job.with_event_listeners(spec.event_listeners)
                |> scheduled_job.create(
                  registry: context.scheduled_jobs_registry,
                  dispatch_registry: context.dispatcher_registry,
                  db_adapter: spec.db_adapter,
                  spec: _,
                )
              })
            })
            |> list.fold(children, supervisor.add)
          }
          // Finally notify the main process we're ready
          |> supervisor.add(supervisor_ready())
        },
      ),
    )

  case supervisor {
    Ok(supervisor) -> {
      let assert Ok(registries.Registries(
        queue_registry,
        dispatcher_registry,
        scheduled_jobs_registry,
      )) = process.receive(self, 500)
      Ok(BgJobs(
        supervisor:,
        queue_registry:,
        dispatcher_registry:,
        scheduled_jobs_registry:,
      ))
    }
    Error(e) -> {
      // TODO: change to send_event
      io.debug(#("Could not create supervisor", e))
      Error(errors.SetupError(e))
    }
  }
}

/// Create the dispatch worker that is responsible for enqueueing all jobs
///
fn create_dispatcher_worker(
  db_adapter: db_adapter.DbAdapter,
  workers: List(worker.Worker),
  event_listners: List(events.EventListener),
) {
  supervisor.worker(fn(context: Context) {
    create_dispatcher(
      registry: context.dispatcher_registry,
      db_adapter:,
      workers:,
      event_listners:,
    )
  })
}

/// Create queue registry supervisor worker
fn supervisor_add_registry_workers(children) {
  children
  |> supervisor.add(
    supervisor.worker(fn(_caller: process.Subject(registries.Registries)) {
      chip.start()
    })
    |> supervisor.returning(
      fn(
        caller: process.Subject(registries.Registries),
        registry: registries.QueueRegistry,
      ) {
        ContextBuilder(caller, registry, option.None, option.None)
      },
    ),
  )
  |> supervisor.add(
    supervisor.worker(fn(_context: ContextBuilder) { chip.start() })
    |> supervisor.returning(
      fn(
        context_builder: ContextBuilder,
        registry: registries.DispatcherRegistry,
      ) {
        ContextBuilder(
          caller: context_builder.caller,
          queue_registry: context_builder.queue_registry,
          dispatcher_registry: option.Some(registry),
          scheduled_jobs_registry: option.None,
        )
      },
    ),
  )
  |> supervisor.add(
    supervisor.worker(fn(_context: ContextBuilder) { chip.start() })
    |> supervisor.returning(
      fn(
        context_builder: ContextBuilder,
        registry: registries.ScheduledJobRegistry,
      ) {
        let assert option.Some(dispatcher_registry) =
          context_builder.dispatcher_registry
        Context(
          caller: context_builder.caller,
          queue_registry: context_builder.queue_registry,
          dispatcher_registry:,
          scheduled_jobs_registry: registry,
        )
      },
    ),
  )
}

/// Send queue registry back to parent 
///
fn supervisor_ready() {
  supervisor.worker(fn(_context: Context) { Ok(process.new_subject()) })
  |> supervisor.returning(fn(context: Context, _self) {
    process.send(
      context.caller,
      registries.Registries(
        context.queue_registry,
        context.dispatcher_registry,
        context.scheduled_jobs_registry,
      ),
    )
    Nil
  })
}

/// Adds a new job to the specified queue.
///
/// ## Example
/// ```gleam
/// enqueue_job("my_queue", "example_job", to_string(ExamplePayload("example")));
/// ```
pub fn enqueue_job(job_request: jobs.JobEnqueueRequest, bg: BgJobs) {
  let assert Ok(queue) = chip.find(bg.dispatcher_registry, "job_dispatcher")
  process.try_call(
    queue,
    messages.EnqueueJob(
      _,
      job_request.name,
      job_request.payload,
      jobs.available_at_from_availability(job_request.availability),
    ),
    1000,
  )
  |> result.replace_error(errors.DispatchJobError("Call error"))
  |> result.flatten
}

pub fn start_processing_all(bg: BgJobs) {
  chip.dispatch(bg.queue_registry, fn(queue) {
    actor.send(queue, messages.StartPolling)
  })
}

pub fn stop_processing_all(bg: BgJobs) {
  chip.dispatch(bg.queue_registry, fn(queue) {
    actor.send(queue, messages.StopPolling)
  })
  chip.dispatch(bg.scheduled_jobs_registry, fn(scheduled_job) {
    actor.send(scheduled_job, scheduled_jobs_messages.StopPolling)
  })
}

/// Sends a message to the specified queue to process jobs.
///
/// This is also sent when enqueueing a job
///
/// ## Example
/// ```gleam
/// process_jobs("my_queue");
/// ```
pub fn process_jobs(queue) {
  process.send(queue, messages.ProcessJobs)
}

// Dispatcher Actor 
//---------------

pub type DispatcherState {
  DispatcherState(
    self: process.Subject(messages.DispatcherMessage),
    db_adapter: db_adapter.DbAdapter,
    send_event: fn(events.Event) -> Nil,
    workers: List(worker.Worker),
  )
}

fn dispatcher_loop(
  message: messages.DispatcherMessage,
  state: DispatcherState,
) -> actor.Next(messages.DispatcherMessage, DispatcherState) {
  case message {
    messages.HandleDispatchError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(events.JobFailedEvent("job_dispatcher", job))
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
            Error(e) ->
              state.send_event(events.QueueErrorEvent("job_dispatcher", e))
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
          state.send_event(events.QueueErrorEvent("job_dispatcher", error))
        }
      }
      actor.continue(state)
    }
  }
}

// QueueActor 
//---------------

pub type QueueState {
  QueueState(
    // state
    is_polling: Bool,
    active_jobs: Int,
    // settings
    self: process.Subject(messages.QueueMessage),
    queue_name: String,
    db_adapter: db_adapter.DbAdapter,
    poll_interval: Int,
    max_retries: Int,
    max_concurrent_jobs: Int,
    send_event: fn(events.Event) -> Nil,
    workers: List(worker.Worker),
  )
}

fn queue_loop(
  message: messages.QueueMessage,
  state: QueueState,
) -> actor.Next(messages.QueueMessage, QueueState) {
  case message {
    messages.StartPolling -> {
      process.send_after(
        state.self,
        state.poll_interval,
        messages.WaitBetweenPoll,
      )
      state.send_event(events.QueuePollingStartedEvent(state.queue_name))
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
      state.send_event(events.JobFailedEvent(state.queue_name, job))
      actor.continue(QueueState(..state, active_jobs: state.active_jobs - 1))
    }

    messages.HandleSuccess(job) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_succeded(job)
      state.send_event(events.JobSuccededEvent(state.queue_name, job))
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
            state.db_adapter.claim_jobs(
              list.map(state.workers, fn(job) { job.job_name }),
              new_jobs_limit,
              state.queue_name,
            )
          {
            Ok(jobs) -> jobs
            Error(err) -> {
              state.send_event(events.QueueErrorEvent(state.queue_name, err))
              panic as "jobs.Job store paniced"
            }
          }

          list.each(jobs, fn(job) {
            state.send_event(events.JobReservedEvent(state.queue_name, job))
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

fn match_worker(workers: List(worker.Worker), job: jobs.Job) {
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

fn run_worker(job: jobs.Job, worker: worker.Worker, state: QueueState) {
  process.start(
    fn() {
      state.send_event(events.JobStartEvent(state.queue_name, job))
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
