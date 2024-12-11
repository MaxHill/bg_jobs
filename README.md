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
  - *otp worker* - monitor (1 per program)
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
import gleam/otp/static_supervisor

fn example_worker() { 
  jobs.Worker(job_name: "example_job", handler: fn(job: jobs.Job) {
      io.print(job.payload)
  })
}

pub fn main() {
  let conn = todo as "the sqlight connection"

  let bg = 
    static_supervisor.new(static_supervisor.OneForOne)
    |> bg_jobs.new(sqlite_db_adapter.new(conn, []))
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

# supervisor
Todo - describe that supervisor is passed in making it possible to customize it.
Example of how to supervise the bg_jobs

# Monitor
The monitor process monitors all queues and scheduled jobs actors using the 
`process.monitor_process()` method from `gleam/erlang/process`. 
If a queue or scheduled job dies a process down signal is sent to the monitor 
and the monitor releases all reservations made by that queue or scheduled job 
in the database. This enables a another (or the same when restarted) to 
claim and process the job.

*To be implemented:* Should something go wrong and this message is 
not received the monitor also periodically loops through the 
reserved jobs in the database (configurable with: some_fn looking 
for jobs reserved dead processes and releases their reservation.

Should the manager itself die all queue and schedule job processes 
are stored in an ETS table and on restart the monitor performs a 
cleanup where it releases all jobs where the reserving process is 
dead aswell as starting to monitor the alive processes again.

# Queue
Each queue that you add to the bg_jobs program spawns an `erlang/otp/
actor` that manages that queue. It's responsible for polling the 
database for jobs it has workers for. 

>*A worker is added to a queue using the `queue.with_worker(Queue, Worker)` 
>method.
>Jobs are added to the database by calling `bg_jobs.enqueue(JobRequest)` 

When a queue actor finds a job it can handle it claims that job by 
setting the `reserved_at` and the `reserved_by` columns in the 
database to the current datetime (UTC+0) and current processes 
pid. This ensures no other queue claims that job 

It then spawns a new process and executed the jobs worker with 
the job data from the database in the new process.

The worker should not panic but instead return a result, if the 
worker panics the queue actor will crash and the supervisor will try 
to restart it.
If the job execution results in an ok a message is sent back to the 
queue actor and the actor moves the job from the jobs table to 
the jobs_succeeded table. 

However if the job execution results in an error, it's retried as 
many times as specified by `queue.with_max_retries` (default: 3).
Should the job not succeed within the max_retries a message is sent 
back to the queue actor and the actor moves the job from the jobs 
table to the jobs_failed table with the string provided in the 
error as the exception.

Multiple queues may have the same workers meaning multiple queues 
may be able to process the same type of jobs. This can be used to 
prioritize job execution. 

For example if one type of job is extra important you could say all 
queues can process that job type by specifying that jobs worker on 
all queues. Or maybe you have a default queue that can handle all 
jobs and then add another queue for a specific job.

Jobs can have delayed processing either at a specific time or 
relative time in the future. This can be specified with the
`bg_jobs.job_with_available_at` or `job_with_available_in` 
respectively.

# Scheduled job
Scheduled jobs work the same way as queues internally with some key 
differences.

Each scheduled job has 1 and only 1 worker meaning it can only 
handle one type of job.

A scheduled job is not manually enqueued. Instead on startup and successfull 
processing of a job the next processing time is calculated and the job is 
enqueued with the available_at value set to that next processing time.

This also means that if the job execution overlaps with what would 
be the next processing time the job wont run that time.

For example if a job starts 13:30 and should run every minute the 
next run time would be 13:31. But if the job execution takes 1min 
30sec. the next run will be at 13:31 since it's only rescheduled 
after it completes.

## Schedules
Schedules for scheduled jobs can be specified in two ways. 
As an interval or as a cron-like schedule

TODO: continue here..
Scheduled jobs can be in two states executing or waiting. It's 
I want to say all jobs start with pausing, meaning a interval 
of every hour will start by waiting for an hour and then execute.
Maybe this should be under interval schedules.

## Interval schedules
  kk

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
