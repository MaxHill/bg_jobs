import bg_jobs
import bg_jobs/job
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervisor
import gleam/result
import singularity
import sqlight

// Errors
pub type BgJobError {
  DbError(sqlight.Error)
  ParseDateError(String)
  DispatchJobError
  Unknown(String)
}

// Worker
pub type Worker =
  fn(job.Job) -> Result(Nil, String)

pub fn match_worker(lookup_table: List(#(String, Worker))) {
  fn(job: job.Job) {
    lookup_table
    |> list.map(fn(in) {
      let #(name, worker) = in
      case name == job.name {
        True -> Ok(worker)
        False -> Error(Nil)
      }
    })
    |> result.values
    |> list.first
    |> option.from_result
  }
}

fn run_worker(
  job: job.Job,
  worker: Worker,
  job_store: JobStore,
  actor: process.Subject(Message(m)),
  max_attempts: Int,
) {
  case worker(job), job.attempts < max_attempts {
    Ok(_), _ -> {
      actor.send(actor, HandleSuccess(job))
    }
    Error(_), True -> {
      let assert Ok(new_job) = job_store.increment_attempts(job)
      run_worker(new_job, worker, job_store, actor, max_attempts)
    }
    Error(err), False -> {
      actor.send(actor, HandleError(job, err))
    }
  }
}

// Queue
pub fn new_otp_worker(
  registry registry,
  queue_type queue_type: fn(process.Subject(Message(job))) -> b,
  max_retries max_retries: Int,
  job_store job_store: JobStore,
  job_mapper job_mapper: fn(job.Job) -> option.Option(Worker),
) {
  let queue_name = bg_jobs.queue_type_name_to_string(queue_type)

  supervisor.worker(fn(_state) {
    new(
      queue_name: queue_name,
      max_retries: max_retries,
      job_store: job_store,
      job_mapper: job_mapper,
    )
    |> result.map(singularity.register(registry, queue_type, subject: _))
  })
}

pub fn new(
  queue_name queue_name: String,
  max_retries max_retries: Int,
  job_store job_store: JobStore,
  job_mapper job_mapper: fn(job.Job) -> option.Option(Worker),
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let actor_subject = process.new_subject()
      let selector =
        process.new_selector()
        |> process.selecting(actor_subject, function.identity)

      let state =
        QueueState(
          queue_name: queue_name,
          job_store: job_store,
          find_worker: job_mapper,
          max_retries: max_retries,
          self: actor_subject,
        )

      actor.Ready(state, selector)
    },
    init_timeout: 1000,
    loop: handle_queue_message,
  ))
}

pub fn registry() {
  singularity.start()
}

pub fn get_queue(registry, job_type) {
  singularity.require(registry, job_type, timeout_ms: 1000)
}

pub fn enqueue_job(queue, name: String, payload: String) {
  process.try_call(queue, DispatchJob(_, name, payload), 1000)
  // TODO: propper logging
  |> result.map_error(fn(e) { io.debug(e) })
  |> result.replace_error(DispatchJobError)
  |> result.flatten
}

pub fn process_jobs(queue) {
  process.send(queue, ProcessJobs)
}

// Store
pub type JobStore {
  JobStore(
    enqueue_job: fn(String, String, String) -> Result(job.Job, BgJobError),
    get_next_jobs: fn(String, Int) -> Result(List(job.Job), BgJobError),
    move_job_to_succeded: fn(job.Job) -> Result(Nil, BgJobError),
    move_job_to_failed: fn(job.Job, String) -> Result(Nil, BgJobError),
    get_succeeded_jobs: fn(Int) -> Result(List(job.SucceededJob), BgJobError),
    get_failed_jobs: fn(Int) -> Result(List(job.FailedJob), BgJobError),
    increment_attempts: fn(job.Job) -> Result(job.Job, BgJobError),
  )
}

// Actor
pub type Message(job_type) {
  DispatchJob(
    reply_with: process.Subject(Result(job.Job, BgJobError)),
    name: String,
    payload: String,
  )
  HandleError(job.Job, exception: String)
  HandleSuccess(job.Job)
  ProcessJobs
}

pub type QueueState(job_type) {
  QueueState(
    queue_name: String,
    job_store: JobStore,
    find_worker: fn(job.Job) -> option.Option(Worker),
    max_retries: Int,
    self: process.Subject(Message(job_type)),
  )
}

fn handle_queue_message(
  message: Message(job_type),
  state: QueueState(job_type),
) -> actor.Next(Message(job_type), QueueState(job_type)) {
  case message {
    DispatchJob(client, job_name, payload) -> {
      let job = state.job_store.enqueue_job(state.queue_name, job_name, payload)
      process.send(client, job)
      actor.send(state.self, ProcessJobs)
      actor.continue(state)
    }
    HandleError(job, exception) -> {
      let assert Ok(_) = state.job_store.move_job_to_failed(job, exception)
      actor.send(state.self, ProcessJobs)
      // remove from db
      // add to failed jobs
      actor.continue(state)
    }
    HandleSuccess(job) -> {
      // Panic and try to restart the service if something goes wrong
      let assert Ok(_) = state.job_store.move_job_to_succeded(job)
      actor.send(state.self, ProcessJobs)
      actor.continue(state)
    }
    ProcessJobs -> {
      let jobs = case state.job_store.get_next_jobs(state.queue_name, 1) {
        Ok(jobs) -> jobs
        // Panic and try to restart the service if something goes wrong
        Error(_) -> panic as "Job store paniced"
      }

      list.each(jobs, fn(job) {
        case state.find_worker(job) {
          option.Some(worker) -> {
            run_worker(
              job,
              worker,
              state.job_store,
              state.self,
              state.max_retries,
            )
            Nil
          }
          option.None -> {
            let assert Ok(_) =
              state.job_store.move_job_to_failed(
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
