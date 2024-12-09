import bg_jobs/db_adapter
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/monitor
import bg_jobs/internal/registries
import bg_jobs/jobs
import bg_jobs/queue
import bg_jobs/scheduled_job
import chip
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/otp/supervisor
import gleam/result
import tempo/duration
import tempo/naive_datetime

/// Main type of the library, holds references queues, and scheduled jobs.
/// This is then passed as an argument when you want to interact with the background 
/// jobs in some way, for example enqueueing 
/// 
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  ...
///  |> bg_jobs.build()
///  
///  bg_jobs.new_job("example_job", "payload")
///  |> bg_jobs.enqueue(bg);
/// ```
pub type BgJobs {
  BgJobs(
    supervisor: process.Subject(supervisor.Message),
    queue_registry: registries.QueueRegistry,
    scheduled_jobs_registry: registries.ScheduledJobRegistry,
    monitor_registry: registries.MonitorRegistry,
    enqueue_state: EnqueueState,
  )
}

pub opaque type EnqueueState {
  EnqueueState(
    workers: List(jobs.Worker),
    db_adapter: db_adapter.DbAdapter,
    send_event: fn(events.Event) -> Nil,
  )
}

// Supervisor
//---------------

type ContextBuilder {
  ContextBuilder(
    caller: process.Subject(registries.Registries),
    queue_registry: registries.QueueRegistry,
    scheduled_jobs_registry: option.Option(registries.ScheduledJobRegistry),
    monitor_registry: option.Option(registries.MonitorRegistry),
  )
}

type Context {
  Context(
    caller: process.Subject(registries.Registries),
    queue_registry: registries.QueueRegistry,
    scheduled_jobs_registry: registries.ScheduledJobRegistry,
    monitor_registry: registries.MonitorRegistry,
  )
}

/// Specification for how the supervisor, queues, scheduled_jobs and 
/// event_listeners should be setup. It's built using the builder functions
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///   // Event listeners
///   |> bg_jobs.with_event_listener(logger_event_listener.listner)
///   // Queues
///   |> bg_jobs.with_queue(queue.new("default_queue"))
///   // Scheduled jobs 
///   |> bg_jobs.with_scheduled_job(scheduled_job.new(
///     cleanup_db_job.worker(),
///     scheduled_job.interval_minutes(1),
///   )) 
///   |> bg_jobs.build()
/// ```
pub type BgJobsSupervisorSpec {
  BgJobsSupervisorSpec(
    db_adapter: db_adapter.DbAdapter,
    event_listeners: List(events.EventListener),
    frequency_period: Int,
    max_frequency: Int,
    queues: List(queue.Spec),
    scheduled_jobs: List(scheduled_job.Spec),
  )
}

/// Create a new default BgJobsSupervisorSpec
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
/// ```
pub fn new(db_adapter: db_adapter.DbAdapter) {
  BgJobsSupervisorSpec(
    db_adapter: db_adapter,
    event_listeners: [],
    max_frequency: 5,
    frequency_period: 1,
    queues: [],
    scheduled_jobs: [],
  )
}

/// Set the supervisors max frequency
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  |> with_supervisor_max_frequency(5)
/// ```
pub fn with_supervisor_max_frequency(
  spec: BgJobsSupervisorSpec,
  max_frequency: Int,
) {
  BgJobsSupervisorSpec(..spec, max_frequency:)
}

/// Set the supervisors frequency period
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  |> with_supervisor_frequency_period(1)
/// ```
pub fn with_supervisor_frequency_period(
  spec: BgJobsSupervisorSpec,
  frequency_period: Int,
) {
  BgJobsSupervisorSpec(..spec, frequency_period:)
}

/// Add an event_listener to all queues under the supervisor
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  |> with_event_listener(logger_event_listener.listener)
/// ```
pub fn with_event_listener(
  spec: BgJobsSupervisorSpec,
  event_listener: events.EventListener,
) {
  BgJobsSupervisorSpec(
    ..spec,
    event_listeners: list.flatten([spec.event_listeners, [event_listener]]),
  )
}

/// Add a queue-spec to create a new queue with the supervisor
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  |> with_queue(queue.new("example_queue") |> queue.add_worker(example_worker))
/// ```
pub fn with_queue(spec: BgJobsSupervisorSpec, queue: queue.Spec) {
  BgJobsSupervisorSpec(..spec, queues: list.flatten([spec.queues, [queue]]))
}

/// Add a scheduled job spec to create a new queue with the supervisor
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  |> with_scheduled_job(scheduled_job.new(
///    example_worker, 
///    scheduled_job.interval_minutes(1)
///  ))
/// ```
pub fn with_scheduled_job(
  spec: BgJobsSupervisorSpec,
  scheduled_job: scheduled_job.Spec,
) {
  BgJobsSupervisorSpec(
    ..spec,
    scheduled_jobs: list.flatten([spec.scheduled_jobs, [scheduled_job]]),
  )
}

/// Create the supervisor and all it's queues based on the provided spec
///
/// ## Example
/// ```gleam
///  let bg = bg_jobs.new(db_adapter)
///  ...
///  |> bg_jobs.build()
///
/// ````
pub fn build(spec: BgJobsSupervisorSpec) -> Result(BgJobs, errors.BgJobError) {
  let self = process.new_subject()

  let all_workers =
    spec.queues
    |> list.map(fn(spec) { spec.workers })
    |> list.flatten
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
          |> registry_workers()
          // Add monitor
          |> supervisor.add(
            supervisor.worker(fn(context: Context) {
              monitor.build(
                registry: context.monitor_registry,
                db_adapter: spec.db_adapter,
              )
            }),
          )
          // Add the queues
          |> fn(children) {
            spec.queues
            |> list.map(fn(queue_spec) {
              supervisor.worker(fn(_context: Context) {
                queue_spec
                |> queue.with_event_listeners(spec.event_listeners)
                |> queue.build(db_adapter: spec.db_adapter, spec: _)
              })
            })
            |> list.fold(children, supervisor.add)
          }
          // Add the scheduled_jobs
          |> fn(children) {
            spec.scheduled_jobs
            |> list.map(fn(scheduled_jobs_spec) {
              supervisor.worker(fn(_context: Context) {
                scheduled_jobs_spec
                |> scheduled_job.with_event_listeners(spec.event_listeners)
                |> scheduled_job.build(db_adapter: spec.db_adapter, spec: _)
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
        scheduled_jobs_registry,
        monitor_registry,
      )) = process.receive(self, 500)
      Ok(BgJobs(
        supervisor:,
        queue_registry:,
        scheduled_jobs_registry:,
        monitor_registry:,
        enqueue_state: EnqueueState(
          workers: all_workers,
          db_adapter: spec.db_adapter,
          send_event: events.send_event(spec.event_listeners, _),
        ),
      ))
    }
    Error(e) -> {
      events.send_event(spec.event_listeners, events.SetupErrorEvent(e))
      Error(errors.SetupError(e))
    }
  }
}

fn registry_workers(children) {
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
        registry: registries.ScheduledJobRegistry,
      ) {
        ContextBuilder(
          caller: context_builder.caller,
          queue_registry: context_builder.queue_registry,
          scheduled_jobs_registry: option.Some(registry),
          monitor_registry: option.None,
        )
      },
    ),
  )
  |> supervisor.add(
    supervisor.worker(fn(_context: ContextBuilder) { chip.start() })
    |> supervisor.returning(
      fn(context_builder: ContextBuilder, registry: registries.MonitorRegistry) {
        let assert option.Some(scheduled_jobs_registry) =
          context_builder.scheduled_jobs_registry
        Context(
          caller: context_builder.caller,
          queue_registry: context_builder.queue_registry,
          scheduled_jobs_registry:,
          monitor_registry: registry,
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
        context.scheduled_jobs_registry,
        context.monitor_registry,
      ),
    )
    Nil
  })
}

// Api
//---------------

// Reexport job

/// Create a new job request that can be enqueued
///
pub fn new_job(name, payload) {
  jobs.new(name, payload)
}

/// Set the availability to a specific time in the future
///
pub fn job_with_available_at(job_request, availabile_at) {
  jobs.with_available_at(job_request, availabile_at)
}

/// Set the availability to a relative time in the future
///
pub fn job_with_available_in(job_request, availabile_in) {
  jobs.with_available_in(job_request, availabile_in)
}

/// Enqueues a job for queues to consume when available
///
/// ## Example
/// ```gleam
/// let bg = bg_jobs.new() |> ... |> bg_jobs.build()
/// bg_jobs.new_job("example_job", "payload")
/// |> bg_jobs.enqueue(bg);
/// ```
pub fn enqueue(job_request: jobs.JobEnqueueRequest, bg: BgJobs) {
  let state = bg.enqueue_state
  use _ <- result.try(
    list.map(state.workers, fn(job) { job.job_name })
    |> list.find(fn(name) { name == job_request.name })
    |> result.map_error(fn(_e) {
      state.send_event(events.NoWorkerForJobError(job_request))
      errors.NoWorkerForJobError(job_request, state.workers)
    }),
  )

  let job =
    state.db_adapter.enqueue_job(
      job_request.name,
      job_request.payload,
      available_at_from_availability(job_request.availability),
    )

  case job {
    Ok(job) -> {
      state.send_event(events.JobEnqueuedEvent(job))
      Ok(job)
    }
    Error(e) -> {
      state.send_event(events.QueueErrorEvent(job_request.name, e))
      Error(e)
    }
  }
}

/// Convert JobAvailability to erlang date time
///
pub fn available_at_from_availability(availability: jobs.JobAvailability) {
  case availability {
    jobs.AvailableNow -> {
      naive_datetime.now_utc() |> naive_datetime.to_tuple()
    }
    jobs.AvailableAt(date) -> date
    jobs.AvailableIn(delay) -> {
      naive_datetime.now_utc()
      |> naive_datetime.add(duration.milliseconds(delay))
      |> naive_datetime.to_tuple()
    }
  }
}
