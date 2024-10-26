import bg_jobs
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/otp/actor

pub type Spec {
  Spec(
    name: String,
    worker: Worker,
    max_retries: Int,
    init_timeout: Int,
    max_concurrent_jobs: Int,
    poll_interval: Int,
    event_listeners: List(bg_jobs.EventListener),
  )
}

pub fn create(
  registry registry: chip.Registry(Message, String, Nil),
  db_adapter db_adapter: bg_jobs.DbAdapter,
  spec spec: Spec,
) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      // Cleanup previously in flight jobs by this queue name
      let assert Ok(_) = bg_jobs.remove_in_flight_jobs(spec.name, db_adapter)

      // Register the queue under a name on initialization
      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(spec.name),
      )

      // Schedule first job
      process.send(self, ScheduleNext)
      // Start polling directly
      process.send(self, StartPolling)

      let state =
        State(
          is_polling: False,
          self:,
          name: spec.name,
          db_adapter:,
          poll_interval: spec.poll_interval,
          max_retries: spec.max_retries,
          send_event: bg_jobs.send_event(spec.event_listeners, _),
          worker: spec.worker,
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
pub type Worker {
  Worker(job_name: String, handler: fn(bg_jobs.Job) -> Result(Nil, String))
}

pub opaque type Message {
  StartPolling
  StopPolling
  WaitBetweenPoll
  ScheduleNext
  HandleError(bg_jobs.Job, exception: String)
  HandleSuccess(bg_jobs.Job)
  ProcessJobs
}

pub type State {
  State(
    // state
    is_polling: Bool,
    // settings
    self: process.Subject(Message),
    name: String,
    db_adapter: bg_jobs.DbAdapter,
    poll_interval: Int,
    max_retries: Int,
    send_event: fn(bg_jobs.Event) -> Nil,
    worker: Worker,
  )
}

fn loop(message: Message, state: State) -> actor.Next(Message, State) {
  case message {
    StartPolling -> {
      process.send_after(state.self, state.poll_interval, WaitBetweenPoll)
      state.send_event(bg_jobs.QueuePollingStartedEvent(state.name))
      actor.continue(State(..state, is_polling: True))
    }

    StopPolling -> {
      actor.continue(State(..state, is_polling: False))
    }

    HandleError(job, exception) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_failed(job, exception)
      state.send_event(bg_jobs.JobFailedEvent(state.name, job))
      actor.send(state.self, ScheduleNext)
      actor.continue(state)
    }

    HandleSuccess(job) -> {
      let assert Ok(_) = state.db_adapter.move_job_to_succeded(job)
      state.send_event(bg_jobs.JobSuccededEvent(state.name, job))
      actor.send(state.self, ScheduleNext)
      actor.continue(state)
    }

    ScheduleNext -> {
      io.debug("Schedule next run")
      actor.send(state.self, WaitBetweenPoll)
      actor.continue(state)
    }

    WaitBetweenPoll -> {
      case state.is_polling {
        True -> {
          process.send_after(state.self, state.poll_interval, ProcessJobs)
          actor.continue(state)
        }
        False -> {
          actor.continue(state)
        }
      }
    }

    ProcessJobs -> {
      let jobs = case
        state.db_adapter.claim_jobs([state.worker.job_name], 1, state.name)
      {
        Ok(jobs) -> jobs
        Error(err) -> {
          state.send_event(bg_jobs.QueueErrorEvent(state.name, err))
          panic as "Job store paniced"
        }
      }

      list.each(jobs, fn(job) {
        state.send_event(bg_jobs.JobReservedEvent(state.name, job))
        run_worker_sync(job, state.worker, state)
      })

      actor.continue(state)
    }
  }
}

fn run_worker_sync(job: bg_jobs.Job, worker: Worker, state: State) {
  state.send_event(bg_jobs.JobStartEvent(state.name, job))
  case worker.handler(job), job.attempts < state.max_retries {
    Ok(_), _ -> {
      actor.send(state.self, HandleSuccess(job))
    }
    Error(_), True -> {
      let assert Ok(new_job) = state.db_adapter.increment_attempts(job)
      run_worker_sync(new_job, worker, state)
    }
    Error(err), False -> {
      actor.send(state.self, HandleError(job, err))
    }
  }
}
