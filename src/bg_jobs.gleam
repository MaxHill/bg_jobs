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
import sqlight
import youid/uuid

// Errors
//---------------

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
  Worker(job_name: String, execute: fn(Job) -> Result(Nil, String))
}

// Job
//---------------

/// It's the data representing a job in the queue.
///
/// It contains all the necessary information to process the job.
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
pub type QueueRegistry =
  chip.Registry(Message, String, Nil)

pub type Context {
  Context(caller: process.Subject(QueueRegistry), registry: QueueRegistry)
}

pub type QueueSupervisorSpec {
  QueueSupervisorSpec(
    max_frequency: Int,
    frequency_period: Int,
    db_adapter: DbAdapter,
    queues: List(QueueSpec),
    event_listners: List(EventListner),
  )
}

pub type QueueSpec {
  QueueSpec(
    name: String,
    workers: List(Worker),
    max_retries: Int,
    init_timeout: Int,
    max_concurrent_jobs: Int,
    poll_interval: Int,
  )
}

pub fn setup(spec: QueueSupervisorSpec) {
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
          |> supervisor.add(registry_otp_worker())
          // Add the dispatch worker
          |> supervisor.add(create_dispatcher_worker(
            spec.db_adapter,
            all_workers,
            spec.event_listners,
          ))
          // Add the queues
          |> fn(children) {
            spec.queues
            |> list.map(fn(queue_spec) {
              supervisor.worker(fn(context: Context) {
                new_queue(
                  registry: context.registry,
                  queue_name: queue_spec.name,
                  max_retries: queue_spec.max_retries,
                  db_adapter: spec.db_adapter,
                  workers: queue_spec.workers,
                  event_listners: spec.event_listners,
                  poll_interval: queue_spec.poll_interval,
                  max_concurrent_jobs: queue_spec.max_concurrent_jobs,
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
      let assert Ok(queue_registry) = process.receive(self, 500)
      Ok(#(supervisor, queue_registry))
    }
    Error(e) -> {
      io.debug("Could not create supervisor")
      Error(SetupError(e))
    }
  }
}

fn create_dispatcher_worker(
  db_adapter: DbAdapter,
  workers: List(Worker),
  event_listners: List(EventListner),
) {
  supervisor.worker(fn(context: Context) {
    new_queue(
      registry: context.registry,
      queue_name: "job_dispatcher",
      max_retries: 3,
      db_adapter: db_adapter,
      workers: workers,
      event_listners: event_listners,
      max_concurrent_jobs: 0,
      poll_interval: 0,
    )
  })
}

/// Create queue registry supervisor worker
fn registry_otp_worker() {
  supervisor.worker(fn(_caller: process.Subject(QueueRegistry)) { chip.start() })
  |> supervisor.returning(fn(caller, registry) { Context(caller, registry) })
}

/// Send back queue registry to parent 
fn supervisor_ready() {
  supervisor.worker(fn(_context: Context) { Ok(process.new_subject()) })
  |> supervisor.returning(fn(context: Context, _self) {
    process.send(context.caller, context.registry)
    Nil
  })
}

/// Creates and starts a new job queue with specified parameters.
///
/// ## Parameters
/// - `registry`: The queue registry for managing queues.
/// - `queue_name`: The name of the queue.
/// - `max_retries`: The maximum number of retries for failed jobs.
/// - `db_adapter`: The database adapter for job management.
/// - `workers`: A list of workers responsible for executing jobs.
///
/// ## Example
/// ```gleam
/// queue.new_queue(
///   registry: context.registry,
///   queue_name: queue_name,
///   max_retries: 3,
///   db_adapter: db_adapter,
///   workers: workers,
/// )
/// ```
pub fn new_queue(
  registry registry: QueueRegistry,
  queue_name queue_name: String,
  max_retries max_retries: Int,
  db_adapter db_adapter: DbAdapter,
  workers workers: List(Worker),
  event_listners event_listners: List(EventListner),
  poll_interval poll_interval: Int,
  max_concurrent_jobs max_concurrent_jobs: Int,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Cleanup previously in flight jobs by this queue name
      let assert Ok(_) = remove_in_flight_jobs(queue_name, db_adapter)

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(queue_name),
      )

      // Start polling directly
      process.send(self, StartPolling)

      let state =
        QueueState(
          queue_name: queue_name,
          db_adapter: db_adapter,
          workers: workers,
          send_event: send_event(event_listners, _),
          poll_interval: poll_interval,
          is_polling: False,
          max_retries: max_retries,
          max_concurrent_jobs: max_concurrent_jobs,
          active_jobs: 0,
          self: self,
        )

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: 1000,
    loop: queue_loop,
  ))
}

fn remove_in_flight_jobs(queue_name: String, db_adapter: DbAdapter) {
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
pub fn enqueue_job(
  queues: process.Subject(chip.Message(Message, String, Nil)),
  name: String,
  payload: String,
  available_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
) {
  let assert Ok(queue) = chip.find(queues, "job_dispatcher")
  process.try_call(queue, EnqueueJob(_, name, payload, available_at), 1000)
  |> result.replace_error(DispatchJobError("Call error"))
  |> result.flatten
}

pub fn start_processing_all(registry: QueueRegistry) {
  chip.dispatch(registry, fn(queue) { actor.send(queue, StartPolling) })
}

pub fn stop_processing_all(registry: QueueRegistry) {
  chip.dispatch(registry, fn(queue) { actor.send(queue, StopPolling) })
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

/// `DbAdapter` defines the interface for database interactions related to job management.
///
/// Each database implementation must provide functions to enqueue, retrieve, and manage jobs.
///
/// ## Example
/// ```gleam
/// pub fn new_sqlite_db_adapter(conn: sqlight.Connection) {
///   queue.DbAdapter(
///     enqueue_job: enqueue_job(conn),
///     get_next_jobs: get_next_jobs(conn),
///     move_job_to_succeded: move_job_to_succeded(conn),
///     move_job_to_failed: move_job_to_failed(conn),
///     get_succeeded_jobs: get_succeeded_jobs(conn),
///     get_failed_jobs: get_failed_jobs(conn),
///     increment_attempts: increment_attempts(conn),
///   )
/// }
/// ```
pub type DbAdapter {
  DbAdapter(
    enqueue_job: fn(
      String,
      String,
      option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    ) ->
      Result(Job, BgJobError),
    get_next_jobs: fn(List(String), Int, String) ->
      Result(List(Job), BgJobError),
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

pub type EventListner =
  fn(Event) -> Nil

pub fn send_event(event_listners: List(EventListner), event: Event) {
  event_listners
  |> list.each(fn(handler) { handler(event) })
}

// Actor
//---------------

pub opaque type Message {
  EnqueueJob(
    reply_with: process.Subject(Result(Job, BgJobError)),
    name: String,
    payload: String,
    available_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
  )
  StartPolling
  StopPolling
  LoopPolling
  HandleError(Job, exception: String)
  HandleSuccess(Job)
  ProcessJobs
}

// TODO: split state and config
pub type QueueState {
  QueueState(
    queue_name: String,
    db_adapter: DbAdapter,
    workers: List(Worker),
    poll_interval: Int,
    is_polling: Bool,
    max_retries: Int,
    max_concurrent_jobs: Int,
    send_event: fn(Event) -> Nil,
    active_jobs: Int,
    self: process.Subject(Message),
  )
}

fn queue_loop(
  message: Message,
  state: QueueState,
) -> actor.Next(Message, QueueState) {
  case message {
    StartPolling -> {
      case state.queue_name {
        "job_dispatcher" -> actor.continue(state)
        _ -> {
          process.send_after(state.self, state.poll_interval, LoopPolling)
          state.send_event(QueuePollingStartedEvent(state.queue_name))
          actor.continue(QueueState(..state, is_polling: True))
        }
      }
    }
    StopPolling -> {
      actor.continue(QueueState(..state, is_polling: False))
    }
    LoopPolling -> {
      case state.is_polling {
        True -> {
          process.send_after(state.self, state.poll_interval, ProcessJobs)
          actor.continue(state)
        }
        False -> actor.continue(state)
      }
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
            Error(e) -> state.send_event(QueueErrorEvent(state.queue_name, e))
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
              available_at: birl.to_erlang_datetime(case available_at {
                option.Some(timestamp) ->
                  birl.from_erlang_universal_datetime(timestamp)
                option.None -> birl.now()
              }),
              reserved_at: option.None,
              reserved_by: option.None,
            )
          actor.send(
            state.self,
            HandleError(job, "Could not enqueue job with no worker"),
          )

          let error = DispatchJobError("No worker for job: " <> job_name)
          process.send(client, Error(error))
          state.send_event(QueueErrorEvent(state.queue_name, error))
        }
      }
      actor.continue(state)
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
          actor.send(state.self, LoopPolling)
          actor.continue(state)
        }
        _ -> {
          let jobs = case
            state.db_adapter.get_next_jobs(
              list.map(state.workers, fn(job) { job.job_name }),
              new_jobs_limit,
              state.queue_name,
            )
          {
            Ok(jobs) -> jobs
            // Panic and try to restart the service if something goes wrong
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
          actor.send(state.self, LoopPolling)
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
      case worker.execute(job), job.attempts < state.max_retries {
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
