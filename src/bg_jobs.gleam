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

// Errors
//---------------

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(sqlight.Error)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(actor.StartError)
  Unknown(String)
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
  )
}

pub type QueueSpec {
  QueueSpec(
    name: String,
    workers: List(Worker),
    max_retries: Int,
    init_timeout: Int,
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
          |> supervisor.add(dispatcher_worker(spec.db_adapter, all_workers))
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

// pub fn supervised_queues(
//   max_frequency max_frequency: Int,
//   frequency_period frequency_period: Int,
//   db_adapter db_adapter: DbAdapter,
//   queues queues: List(supervisor.ChildSpec(Message, Context, Context)),
// ) {
//   let self = process.new_subject()
//
//   case
//     supervisor.start_spec(
//       supervisor.Spec(
//         argument: self,
//         max_frequency: max_frequency,
//         frequency_period: frequency_period,
//         init: fn(children) {
//           children
//           // First spawn the registry
//           |> supervisor.add(registry_otp_worker())
//           |> supervisor.add(dispatcher_worker(db_adapter))
//           // Add the queues
//           |> fn(children) {
//             queues
//             |> list.fold(children, supervisor.add)
//           }
//           // Finally notify the main process we're ready
//           |> supervisor.add(supervisor_ready())
//         },
//       ),
//     )
//   {
//     Ok(supervisor) -> {
//       let assert Ok(queue_registry) = process.receive(self, 500)
//       Ok(#(supervisor, queue_registry))
//     }
//     Error(e) -> {
//       io.debug("Could not create supervisor")
//       Error(SetupError(e))
//     }
//   }
// }

fn dispatcher_worker(db_adapter: DbAdapter, workers: List(Worker)) {
  supervisor.worker(fn(context: Context) {
    new_queue(
      registry: context.registry,
      queue_name: "job_dispatcher",
      max_retries: 3,
      db_adapter: db_adapter,
      workers: workers,
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
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(queue_name),
      )

      let state =
        QueueState(
          queue_name: queue_name,
          db_adapter: db_adapter,
          workers: workers,
          max_retries: max_retries,
          self: self,
        )

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: 1000,
    loop: handle_queue_message,
  ))
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
) {
  let assert Ok(queue) = chip.find(queues, "job_dispatcher")
  process.try_call(queue, DispatchJob(_, name, payload), 1000)
  // TODO: propper logging
  |> result.map_error(fn(e) { io.debug(e) })
  |> result.replace_error(DispatchJobError("Call error"))
  |> result.flatten
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
    enqueue_job: fn(String, String) -> Result(Job, BgJobError),
    get_next_jobs: fn(List(String), Int) -> Result(List(Job), BgJobError),
    move_job_to_succeded: fn(Job) -> Result(Nil, BgJobError),
    move_job_to_failed: fn(Job, String) -> Result(Nil, BgJobError),
    get_succeeded_jobs: fn(Int) -> Result(List(SucceededJob), BgJobError),
    get_failed_jobs: fn(Int) -> Result(List(FailedJob), BgJobError),
    increment_attempts: fn(Job) -> Result(Job, BgJobError),
    migrate_up: fn() -> Result(Nil, BgJobError),
    migrate_down: fn() -> Result(Nil, BgJobError),
  )
}

// Actor
//---------------

pub opaque type Message {
  DispatchJob(
    reply_with: process.Subject(Result(Job, BgJobError)),
    name: String,
    payload: String,
  )
  HandleError(Job, exception: String)
  HandleSuccess(Job)
  ProcessJobs
}

pub type QueueState {
  QueueState(
    queue_name: String,
    db_adapter: DbAdapter,
    workers: List(Worker),
    max_retries: Int,
    self: process.Subject(Message),
  )
}

fn handle_queue_message(
  message: Message,
  state: QueueState,
) -> actor.Next(Message, QueueState) {
  case message {
    DispatchJob(client, job_name, payload) -> {
      let worker_job_name =
        list.map(state.workers, fn(job) { job.job_name })
        |> list.find(fn(name) { name == job_name })

      case worker_job_name {
        Ok(name) -> {
          let job = state.db_adapter.enqueue_job(name, payload)
          process.send(client, job)
        }
        Error(_) -> {
          let err = Error(DispatchJobError("No worker for job: " <> job_name))
          process.send(client, err)
        }
      }
      actor.continue(state)
    }
    HandleError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      actor.send(state.self, ProcessJobs)
      // remove from db
      // add to failed jobs
      actor.continue(state)
    }
    HandleSuccess(job) -> {
      // Panic and try to restart the service if something goes wrong
      let assert Ok(_) = state.db_adapter.move_job_to_succeded(job)
      actor.send(state.self, ProcessJobs)
      actor.continue(state)
    }
    ProcessJobs -> {
      let jobs = case
        state.db_adapter.get_next_jobs(
          list.map(state.workers, fn(job) { job.job_name }),
          2,
        )
      {
        Ok(jobs) -> jobs
        // Panic and try to restart the service if something goes wrong
        Error(_) -> panic as "Job store paniced"
      }

      list.each(jobs, fn(job) {
        case match_worker(state.workers, job) {
          option.Some(worker) -> {
            run_worker(
              job,
              worker,
              state.db_adapter,
              state.self,
              state.max_retries,
            )
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

      actor.continue(state)
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

fn run_worker(
  job: Job,
  worker: Worker,
  db_adapter: DbAdapter,
  actor: process.Subject(Message),
  max_attempts: Int,
) {
  case worker.execute(job), job.attempts < max_attempts {
    Ok(_), _ -> {
      actor.send(actor, HandleSuccess(job))
    }
    Error(_), True -> {
      let assert Ok(new_job) = db_adapter.increment_attempts(job)
      run_worker(new_job, worker, db_adapter, actor, max_attempts)
    }
    Error(err), False -> {
      actor.send(actor, HandleError(job, err))
    }
  }
}

// FFI
//---------------

/// Extracts the name of type from the constructor.
///
@external(erlang, "bg_jobs_ffi", "queue_type_name_to_string")
pub fn queue_type_name_to_string(
  varfn: fn(process.Subject(msg)) -> wrap,
) -> String

/// Extracts the name of type from the constructor.
///
@external(erlang, "bg_jobs_ffi", "function_name_to_string")
pub fn function_name_to_string(fun: fn(any) -> something) -> String
