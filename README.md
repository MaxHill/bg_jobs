# bg_jobs - Background job processing for your Gleam app
[![Package Version](https://img.shields.io/hexpm/v/bg_jobs)](https://hex.pm/packages/bg_jobs)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/bg_jobs/)

# What is bg_jobs
When developing web applications, certain tasks, like processing large files or 
performing heavy calculations can slow down regular request handling. 
bg_jobs helps by moving these time-consuming operations to a background 
queue, keeping your application responsive.

This library is SQL-driven, with adapters for sqlite and postgres included. 
So you don’t need extra dependencies to get background processing up 
and running. Just connect to your existing database, set up your 
background jobs, and your app can handle more demanding tasks without 
impacting user experience.

Bg_jobs provide the following:
- *Queues* - including delayed execution
- *Scheduled jobs* - on an interval or cron like schedule

# Architecture
*TODO:* expand this section

Program architecture:
- *otp supervisor*
  - *otp worker* - queue (0 or more)
    - Polls the database on specified interval looking for avaliable jobs of the type it can handle
  - *otp worker* - scheduled jobs queue (0 or more)
    - Polls the database on specified interval looking for avaliable jobs of the type it can handle
    - Enqueus a new job with apropriate avalible at after processing is done 
  - *otp worker* - chip registry (1 per program)
    - Holds Subjects to all workers in bg_jobs. This is used for named lookup

## Durability
Jobs are claimed with a timestamp and queue/scheduled job name.
Each queue and scheduled job should have a unique name even across machines.
Whenever the worker starts up it clears out previously claimed jobs.
This way if a worker crashes and the supervisor restarts it there is no 
abandoned jobs.


# Time and date
All dates should be passed in utc+0

# Installation 

```sh
gleam add bg_jobs
```
# Example usage 
A complete example setup can be found in the /example directory.
```gleam
import bg_jobs
import bg_jobs/job
import bg_jobs/queue

fn example_worker() { 
  jobs.Worker(job_name: "example_job", handler: fn(job: jobs.Job) {
      io.print(job.payload)
  })
}

pub fn main() {
  let conn = todo as "the sqlight connection"

  let bg = bg_jobs.new(sqlite_db_adapter.new(conn, []))
  |> bg_jobs.with_queue(
    queue.new("default_queue")
    |> queue.with_worker(example_worker())
  )
  |> bg_jobs.build()

  // Dispatch a new job
  jobs.new(example_worker.job_name, "Hello!"))
  |> bg_jobs.enqueue(bg)

  process.sleep_forever()
}
```
This example sets up a `default_queue` to handle `example_worker` jobs. 
When a job with the payload "Hello!" is dispatched, it is stored in the 
database, picked up by the queue, and logged.

# Core Components 
## BgJobs
BgJobs is the main entry point for setting up background job 
processing. This instance manages queues, scheduled jobs, and event 
listeners. After initialization, all interactions with bg_jobs are 
performed through this instance.

Setup with configurable options:
```gleam
  let bg = bg_jobs.new(db_adapter)
  |> with_supervisor_max_frequency(5)
  |> with_supervisor_frequency_period(1)
  // Event listeners
  |> bg_jobs.with_event_listener(logger_event_listener.listner)
  // Queues
  |> bg_jobs.with_queue(queue.new("default_queue"))
  // Scheduled jobs 
  |> bg_jobs.with_scheduled_job(scheduled_job.new(
    cleanup_db_job.worker(),
    scheduled_job.interval_minutes(1),
  )) 
  |> bg_jobs.build()
```

## Queues
Queues handle the processing of jobs in the background. They poll the 
database at a specified interval and pick up jobs that are ready for 
processing. Jobs are routed to the appropriate workers based on the job 
name, and each queue can be customized for job concurrency, polling 
interval, retries, and worker configuration.

Queue setup with configurable options:
```gleam
  queue.new(queue_name)
  |> queue.with_max_concurrent_jobs(10)
  |> queue.with_poll_interval_ms(100)
  |> queue.with_max_concurrent_jobs(10)
  |> queue.with_max_retries(3)
  |> queue.with_init_timeout(1000)
  |> queue.with_workers([...])
  |> queue.with_worker(...)
  |> queue.with_event_listeners([...])
  |> queue.with_event_listener(...)
  |> queue.build()
```

## Scheduled jobs
Scheduled jobs are jobs that run on a predefined schedule, either at a 
fixed interval (e.g., every 5 minutes) or a more complex schedule (e.g
., cron-like schedules). They self-manage their scheduling, calculating 
the next run time after each execution. A scheduled job will become available 
once it reaches the specified time, but when it runs may depend on 
queue availability.

Scheduled job options:
- Interval Scheduling: Jobs run at a regular interval, for example every minute.
- Cron-like Scheduling: Supports complex scheduling, where you can set 
  specific execution times.

Intervall scheduled job: 
```gleam
// Interval-based job that runs every minute
scheduled_job.new(
    cleanup_db_job.worker(),
    scheduled_job.interval_minutes(1)
  )

// Cron-like schedule to run at the 10th second and 10th minute of every hour
scheduled_job.new(
    delete_expired_sessions_job.worker(),
    scheduled_job.Schedule(
      scheduled_job.new_schedule()
      |> scheduled_job.on_second(10)
      |> scheduled_job.on_minute(10)
    )
  )
```
*Note*: The scheduled time is when the job becomes available to the 
queue, but the exact run time depends on the scheduled_jobs polling 
interval and speed


## Event Listeners
bg_jobs generates events during job processing, which can be used for 
logging, monitoring, or custom telemetry. An event listener can be 
added globally to capture all job events, or it can be attached to 
specific queues or scheduled jobs for targeted monitoring.

To use the built-in logger_event_listener:
```gleam
bg_jobs.with_event_listener(logger_event_listener.listener)  // Logs events for all job processing events
```

Creating a Custom Event Listener

Implement a custom event listener by defining a function with the 
following signature:
```gleam
pub type EventListener = fn(Event) -> Nil
```

This function will be called whenever an event occurs, receiving an Event 
object as its parameter. You can then pass your custom listener to 
bg_jobs (global listener), a specific queue, or a scheduled job as 
needed.

Example of adding a custom event listener to a specific queue:
```gleam
queue.with_event_listener(my_custom_event_listener())
```

## Db adapters
bg_jobs includes two built-in database adapters: Postgres (via pog) and 
SQLite (via sqlight). These adapters allow seamless integration with 
popular databases, enabling you to use background job processing 
without additional dependencies. If you are using a different database, 
refer to these implementations for guidance on building a compatible 
adapter.

Adding a database adapter to bg_jobs:
```gleam
let bg = bg_jobs.new(sqlite_db_adapter.new(conn, []))
```

Further documentation can be found at <https://hexdocs.pm/bg_jobs>.
## Development

```sh
gleam run   # Run the project
gleam test  # Run the tests
just watch-test # Run tests in watch mode
```

# TODO
- [x] Rename duration to clock / time something like that 
- [x] Look over the public api so it makes sense
- [x] Make events module public
- [ ] Document architecture
- [ ] Document scheduling
- [ ] Split db adapters to their own packages
  Need to solve testing, since the tests depend on sqlite
- [ ] Replace [chip](https://hexdocs.pm/chip/) with custom solution
  Custom worker repository that also monitors and releases claimed jobs if a worker dies. 
  This is handled today with the worker releasing claims when starting.
  With the monitoring approach the worker does not have to come alive to prevent abandoned jobs.
  (And we could also drop the queue-name probably)
