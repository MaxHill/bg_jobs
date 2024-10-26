import birl
import birl/duration
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervisor
import gleam/result
import sqlight
import youid/uuid

// Errors
//---------------
pub type BgJobs {
  BgJobs(
    supervisor: process.Subject(supervisor.Message),
    queue_registry: QueueRegistry,
    dispatcher_registry: DispatcherRegistry,
    scheduled_jobs_registry: QueueRegistry,
  )
}

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(sqlight.Error)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(actor.StartError)
  UnknownError(String)
}

// Worker
//---------------

/// Represents a job worker responsible for executing specific tasks.
///
/// Each job must implement this type, defining the job name and its execution logic.
///
/// ## Example
///
/// ```gleam
/// pub fn worker(email_service: SomeService) {
///   Worker(job_name: "send_email", execute: fn(job) { 
///     from_string(job) 
///     |> email_service.send
///   })
/// }
/// ```
pub type Worker {
  Worker(job_name: String, handler: fn(Job) -> Result(Nil, String))
}

// Job
//---------------

/// It's the data representing a job in the queue.
///
/// It contains all the necessary information to process the job.
///
pub type Job {
  Job(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: option.Option(String),
  )
}

/// Enum representing 3 ways of setting a jobs available_at 
///
pub type JobAvailability {
  AvailableNow
  AvailableAt(#(#(Int, Int, Int), #(Int, Int, Int)))
  AvailableIn(Int)
}

/// Data that is needed to enqueue a new job
///
pub type JobEnqueueRequest {
  JobEnqueueRequest(
    name: String,
    payload: String,
    availability: JobAvailability,
  )
}

/// Create a new job_request
///
pub fn new_job(name: String, payload: String) {
  JobEnqueueRequest(name:, payload:, availability: AvailableNow)
}

/// Set the availability of the job_request to a specific date-time
///
pub fn job_with_available_at(
  job_request: JobEnqueueRequest,
  availabile_at: #(#(Int, Int, Int), #(Int, Int, Int)),
) {
  JobEnqueueRequest(..job_request, availability: AvailableAt(availabile_at))
}

/// Set the availability of the job_request to a time in the future
///
pub fn job_with_available_in(job_request: JobEnqueueRequest, availabile_in: Int) {
  JobEnqueueRequest(..job_request, availability: AvailableIn(availabile_in))
}

/// Convert JobAvailability to erlang date time
///
fn available_at_from_availability(availability: JobAvailability) {
  case availability {
    AvailableNow -> {
      birl.now() |> birl.to_erlang_datetime()
    }
    AvailableAt(date) -> {
      date
      |> birl.from_erlang_local_datetime()
      |> birl.to_erlang_datetime()
    }
    AvailableIn(delay) -> {
      birl.now()
      |> birl.add(duration.milli_seconds(delay))
      |> birl.to_erlang_datetime()
    }
  }
}

/// Holds data about the outcome of processed a job.
///
/// It can either be a successful job or a failed job.
pub type SucceededJob {
  SucceededJob(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    succeded_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

pub type FailedJob {
  FailedJob(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    failed_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

// Queue
//---------------
type QueueRegistry =
  chip.Registry(QueueMessage, String, Nil)

type DispatcherRegistry =
  chip.Registry(DispatcherMessage, String, Nil)

type Registries {
  Registries(
    queue_registry: QueueRegistry,
    dispatcher_registry: DispatcherRegistry,
    scheduled_jobs_registry: QueueRegistry,
  )
}

type ContextBuilder {
  ContextBuilder(
    caller: process.Subject(Registries),
    queue_registry: QueueRegistry,
    dispatcher_registry: option.Option(DispatcherRegistry),
    scheduled_jobs_registry: option.Option(QueueRegistry),
  )
}

type Context {
  Context(
    caller: process.Subject(Registries),
    queue_registry: QueueRegistry,
    dispatcher_registry: DispatcherRegistry,
    scheduled_jobs_registry: QueueRegistry,
  )
}

pub type QueueSupervisorSpec {
  QueueSupervisorSpec(
    max_frequency: Int,
    frequency_period: Int,
    db_adapter: DbAdapter,
    queues: List(QueueSpec),
    event_listeners: List(EventListener),
  )
}

/// Start building a new spec for bg_jobs.
///
pub fn new(db_adapter: DbAdapter) {
  QueueSupervisorSpec(
    max_frequency: 5,
    frequency_period: 1,
    event_listeners: [],
    db_adapter: db_adapter,
    queues: [],
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
  event_listener: EventListener,
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

/// Represents a queue that should be created under the queue supervisor
pub type QueueSpec {
  QueueSpec(
    name: String,
    workers: List(Worker),
    max_retries: Int,
    init_timeout: Int,
    max_concurrent_jobs: Int,
    poll_interval: Int,
    event_listeners: List(EventListener),
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
pub fn queue_with_workers(spec: QueueSpec, workers: List(Worker)) {
  QueueSpec(..spec, workers:)
}

/// Add a worker to the list the queue will poll for and run
///
pub fn queue_add_worker(spec: QueueSpec, worker: Worker) {
  QueueSpec(..spec, workers: list.concat([spec.workers, [worker]]))
}

/// Set event listeners for the queue
/// NOTE: This will override previously added workers
///
pub fn queue_with_event_listeners(
  spec: QueueSpec,
  event_listeners: List(EventListener),
) {
  QueueSpec(..spec, event_listeners:)
}

/// Append a list of event listeners for the queue
///
pub fn queue_add_event_listeners(
  spec: QueueSpec,
  event_listeners: List(EventListener),
) {
  QueueSpec(
    ..spec,
    event_listeners: list.concat([spec.event_listeners, event_listeners]),
  )
}

/// Add a event listener to the list of event listners for the queue
///
pub fn queue_add_event_listener(spec: QueueSpec, event_listener: EventListener) {
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
  registry registry: QueueRegistry,
  db_adapter db_adapter: DbAdapter,
  queue_spec spec: QueueSpec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Cleanup previously in flight jobs by this queue name
      let assert Ok(_) = remove_in_flight_jobs(spec.name, db_adapter)

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(spec.name),
      )

      // Start polling directly
      process.send(self, StartPolling)

      let state =
        QueueState(
          queue_name: spec.name,
          db_adapter: db_adapter,
          workers: spec.workers,
          send_event: send_event(spec.event_listeners, _),
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
  registry registry: DispatcherRegistry,
  db_adapter db_adapter: DbAdapter,
  workers workers: List(Worker),
  event_listners event_listeners: List(EventListener),
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
          send_event: send_event(event_listeners, _),
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
pub fn create(spec: QueueSupervisorSpec) -> Result(BgJobs, BgJobError) {
  let self = process.new_subject()

  let all_workers =
    spec.queues
    |> list.map(fn(spec) { spec.workers })
    |> list.concat

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
          // Finally notify the main process we're ready
          |> supervisor.add(supervisor_ready())
        },
      ),
    )

  case supervisor {
    Ok(supervisor) -> {
      let assert Ok(Registries(
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
      Error(SetupError(e))
    }
  }
}

/// Create the dispatch worker that is responsible for enqueueing all jobs
///
fn create_dispatcher_worker(
  db_adapter: DbAdapter,
  workers: List(Worker),
  event_listners: List(EventListener),
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
    supervisor.worker(fn(_caller: process.Subject(Registries)) { chip.start() })
    |> supervisor.returning(
      fn(caller: process.Subject(Registries), registry: QueueRegistry) {
        ContextBuilder(caller, registry, option.None, option.None)
      },
    ),
  )
  |> supervisor.add(
    supervisor.worker(fn(_context: ContextBuilder) { chip.start() })
    |> supervisor.returning(
      fn(context_builder: ContextBuilder, registry: DispatcherRegistry) {
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
      fn(context_builder: ContextBuilder, registry: QueueRegistry) {
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
      Registries(
        context.queue_registry,
        context.dispatcher_registry,
        context.scheduled_jobs_registry,
      ),
    )
    Nil
  })
}

/// Remove in flight jobs to avoid orphaned jobs.
/// This would happen if a queue has active jobs and get's restarted, 
/// either because of a new deploy or a panic
///
pub fn remove_in_flight_jobs(queue_name: String, db_adapter: DbAdapter) {
  db_adapter.get_running_jobs(queue_name)
  |> result.map(fn(jobs) {
    jobs
    |> list.map(fn(job) { job.id })
    |> list.map(db_adapter.release_claim(_))
  })
}

/// Adds a new job to the specified queue.
///
/// ## Example
/// ```gleam
/// enqueue_job("my_queue", "example_job", to_string(ExamplePayload("example")));
/// ```
pub fn enqueue_job(job_request: JobEnqueueRequest, bg: BgJobs) {
  let assert Ok(queue) = chip.find(bg.dispatcher_registry, "job_dispatcher")
  process.try_call(
    queue,
    EnqueueJob(
      _,
      job_request.name,
      job_request.payload,
      available_at_from_availability(job_request.availability),
    ),
    1000,
  )
  |> result.replace_error(DispatchJobError("Call error"))
  |> result.flatten
}

pub fn start_processing_all(bg: BgJobs) {
  chip.dispatch(bg.queue_registry, fn(queue) { actor.send(queue, StartPolling) })
}

pub fn stop_processing_all(bg: BgJobs) {
  chip.dispatch(bg.queue_registry, fn(queue) { actor.send(queue, StopPolling) })
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
  process.send(queue, ProcessJobs)
}

// Database
//---------------

/// Defines the interface for database interactions related to job management.
///
/// Each database implementation must provide functions to enqueue, retrieve, and manage jobs.
///
pub type DbAdapter {
  DbAdapter(
    enqueue_job: fn(String, String, #(#(Int, Int, Int), #(Int, Int, Int))) ->
      Result(Job, BgJobError),
    claim_jobs: fn(List(String), Int, String) -> Result(List(Job), BgJobError),
    release_claim: fn(String) -> Result(Job, BgJobError),
    move_job_to_succeded: fn(Job) -> Result(Nil, BgJobError),
    move_job_to_failed: fn(Job, String) -> Result(Nil, BgJobError),
    get_succeeded_jobs: fn(Int) -> Result(List(SucceededJob), BgJobError),
    get_failed_jobs: fn(Int) -> Result(List(FailedJob), BgJobError),
    get_running_jobs: fn(String) -> Result(List(Job), BgJobError),
    increment_attempts: fn(Job) -> Result(Job, BgJobError),
    migrate_up: fn() -> Result(Nil, BgJobError),
    migrate_down: fn() -> Result(Nil, BgJobError),
  )
}

// Event
//---------------
pub type Event {
  JobEnqueuedEvent(job: Job)
  JobReservedEvent(queue_name: String, job: Job)
  JobStartEvent(queue_name: String, job: Job)
  JobSuccededEvent(queue_name: String, job: Job)
  JobFailedEvent(queue_name: String, job: Job)
  QueuePollingStartedEvent(queue_name: String)
  QueuePollingStopedEvent(queue_name: String)
  QueueErrorEvent(queue_name: String, error: BgJobError)
  DbQueryEvent(sql: String, attributes: List(String))
  DbResponseEvent(response: String)
  DbErrorEvent(error: BgJobError)
}

pub type EventListener =
  fn(Event) -> Nil

pub fn send_event(event_listners: List(EventListener), event: Event) {
  event_listners
  |> list.each(fn(handler) { handler(event) })
}

// Dispatcher Actor 
//---------------

pub opaque type DispatcherMessage {
  EnqueueJob(
    reply_with: process.Subject(Result(Job, BgJobError)),
    name: String,
    payload: String,
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
  HandleDispatchError(Job, exception: String)
}

pub type DispatcherState {
  DispatcherState(
    self: process.Subject(DispatcherMessage),
    db_adapter: DbAdapter,
    send_event: fn(Event) -> Nil,
    workers: List(Worker),
  )
}

fn dispatcher_loop(
  message: DispatcherMessage,
  state: DispatcherState,
) -> actor.Next(DispatcherMessage, DispatcherState) {
  case message {
    HandleDispatchError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(JobFailedEvent("job_dispatcher", job))
      actor.continue(state)
    }
    EnqueueJob(client, job_name, payload, available_at) -> {
      let has_worker_for_job_name =
        list.map(state.workers, fn(job) { job.job_name })
        |> list.find(fn(name) { name == job_name })

      case has_worker_for_job_name {
        Ok(name) -> {
          let job = state.db_adapter.enqueue_job(name, payload, available_at)
          process.send(client, job)
          case job {
            Ok(job) -> state.send_event(JobEnqueuedEvent(job))
            Error(e) -> state.send_event(QueueErrorEvent("job_dispatcher", e))
          }
        }
        Error(_) -> {
          let job =
            Job(
              id: uuid.v4_string(),
              name: job_name,
              payload: payload,
              attempts: 0,
              created_at: birl.to_erlang_datetime(birl.now()),
              available_at: birl.to_erlang_datetime(
                birl.from_erlang_universal_datetime(available_at),
              ),
              reserved_at: option.None,
              reserved_by: option.None,
            )
          actor.send(
            state.self,
            HandleDispatchError(job, "Could not enqueue job with no worker"),
          )

          let error = DispatchJobError("No worker for job: " <> job_name)
          process.send(client, Error(error))
          state.send_event(QueueErrorEvent("job_dispatcher", error))
        }
      }
      actor.continue(state)
    }
  }
}

// QueueActor 
//---------------

pub opaque type QueueMessage {
  StartPolling
  StopPolling
  WaitBetweenPoll
  HandleError(Job, exception: String)
  HandleSuccess(Job)
  ProcessJobs
}

pub type QueueState {
  QueueState(
    // state
    is_polling: Bool,
    active_jobs: Int,
    // settings
    self: process.Subject(QueueMessage),
    queue_name: String,
    db_adapter: DbAdapter,
    poll_interval: Int,
    max_retries: Int,
    max_concurrent_jobs: Int,
    send_event: fn(Event) -> Nil,
    workers: List(Worker),
  )
}

fn queue_loop(
  message: QueueMessage,
  state: QueueState,
) -> actor.Next(QueueMessage, QueueState) {
  case message {
    StartPolling -> {
      process.send_after(state.self, state.poll_interval, WaitBetweenPoll)
      state.send_event(QueuePollingStartedEvent(state.queue_name))
      actor.continue(QueueState(..state, is_polling: True))
    }

    StopPolling -> {
      actor.continue(QueueState(..state, is_polling: False))
    }

    WaitBetweenPoll -> {
      case state.is_polling {
        True -> {
          process.send_after(state.self, state.poll_interval, ProcessJobs)
          actor.continue(state)
        }
        False -> actor.continue(state)
      }
    }

    HandleError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(JobFailedEvent(state.queue_name, job))
      actor.continue(QueueState(..state, active_jobs: state.active_jobs - 1))
    }

    HandleSuccess(job) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_succeded(job)
      state.send_event(JobSuccededEvent(state.queue_name, job))
      actor.continue(QueueState(..state, active_jobs: state.active_jobs - 1))
    }

    ProcessJobs -> {
      let new_jobs_limit = state.max_concurrent_jobs - state.active_jobs
      case new_jobs_limit {
        0 -> {
          actor.send(state.self, WaitBetweenPoll)
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
              state.send_event(QueueErrorEvent(state.queue_name, err))
              panic as "Job store paniced"
            }
          }

          list.each(jobs, fn(job) {
            state.send_event(JobReservedEvent(state.queue_name, job))
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
          actor.send(state.self, WaitBetweenPoll)
          actor.continue(QueueState(..state, active_jobs:))
        }
      }
    }
  }
}

fn match_worker(workers: List(Worker), job: Job) {
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

fn run_worker(job: Job, worker: Worker, state: QueueState) {
  process.start(
    fn() {
      state.send_event(JobStartEvent(state.queue_name, job))
      case worker.handler(job), job.attempts < state.max_retries {
        Ok(_), _ -> {
          actor.send(state.self, HandleSuccess(job))
        }
        Error(_), True -> {
          let assert Ok(new_job) = state.db_adapter.increment_attempts(job)
          run_worker(new_job, worker, state)
        }
        Error(err), False -> {
          actor.send(state.self, HandleError(job, err))
        }
      }
    },
    False,
  )
  Nil
}
