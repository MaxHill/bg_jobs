# bg_jobs - Background job processing for your Gleam app
[![Package Version](https://img.shields.io/hexpm/v/bg_jobs)](https://hex.pm/packages/bg_jobs)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/bg_jobs/)

# TODO before version 1
- [ ] Generally improve documentation
- [ ] Split db adapters to their own packages
  - To do this testing needs change since the tests depend on the sqlite adapter
- [ ] In the monitor module, `release abandoned jobs` should run on an interval to
  handle the case where the down message is not received 
---
# Other improvements
- [ ] Mock time implementation to not have to sleep in tests

# Docs are WIP right now
Most of it is correct but not complete, some parts are duplicated.

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


# Time and date
All dates should be passed in UTC+0

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
TODO: update, this is not exactly correct any more..
~~Jobs are claimed with a timestamp and queue/scheduled job name.
Each queue and scheduled job should have a unique name even across machines.
Whenever the worker starts up it clears out previously claimed jobs.
This way if a worker crashes and the supervisor restarts it there is no 
abandoned jobs.~~


# Supervisor Integration in bg_jobs

bg_jobs is an OTP-based job scheduling system that integrates with 
Erlang’s supervision tree. When setting up bg_jobs, you need to pass 
a `static_supervisor.Builder` along with a db_adapter. This provides 
flexibility, allowing you to configure the supervisor's settings 
before passing it into bg_jobs. You can also choose to supervise the 
created supervisor itself.

## Configuring the Supervisor for bg_jobs

To set custom parameters for the bg_jobs supervisor, you need to 
first create the supervisor with the desired strategy and 
configuration settings. 

**Example 1: Setting restart_tolerance for the bg_jobs Supervisor**
You can configure the restart_tolerance setting for the supervisor, 
which determines how many restarts are tolerated in a given time 
window before the supervisor itself is terminated.
```gleam
static_supervisor.new(static_supervisor.OneForOne)   // Create a new supervisor with the OneForOne strategy
|> static_supervisor.restart_tolerance(10, 1)  // Set restart tolerance to 10 restarts in 1 second
|> bg_jobs.new(db_adapter)  // Create a new bg_jobs instance with the specified db_adapter
|> bg_jobs.build()  // Build and start the bg_jobs supervision tree
```
In this example:
- `sup.new(sup.OneForOne)` creates a new supervisor with the OneForOne strategy.
- `sup.restart_tolerance(100, 1)` configures the supervisor to tolerate 100 restarts within a 1-second window.
- `bg_jobs.new(db_adapter)` creates a new instance of bg_jobs, passing in the necessary db_adapter.
- `bg_jobs.build()` builds and starts the bg_jobs system under the supervision tree.

**Example 2: Supervising the bg_jobs Supervisor**
You can also add the bg_jobs supervisor as a child to another 
supervisor, allowing you to control its lifecycle and supervise it 
as part of your system.
```gleam
fn setup_bg_jobs() -> Result(bg_jobs.BgJobs, errors.BgJobError) {
  // Define how the bg_jobs supervisor is set up
  todo
}

static_supervisor.new(sup.OneForOne)   // Create a new supervisor with the OneForOne strategy
|> static_supervisor.add(static_supervisor.supervisor_child("bg_jobs", fn() {  // Add the bg_jobs supervisor as a child
  setup_bg_jobs()  // Set up bg_jobs
    |> result.map(fn(bg) { bg.supervisor })  // Map the result to return the supervisors pid
}))
```
In this example:
- `sup.new(sup.OneForOne)` creates a new supervisor with the OneForOne strategy.
- `sup.add(sup.supervisor_child("bg_jobs", fn() { ... }))` adds the bg_jobs supervisor as a child of the main supervisor.
- `setup_bg_jobs()` contains the logic for setting up the bg_jobs system, which could include creating job schedulers and configuring database adapters.
- `result.map(fn(bg) { bg.supervisor })` ensures that the supervisor for the bg_jobs system is returned and supervised correctly.

---
# Monitor Process in bg_jobs

The Monitor process in bg_jobs is responsible for overseeing the 
health of all queues and scheduled job actors. It ensures that if 
any job or queue process dies unexpectedly, the corresponding 
reservation in the database is released, allowing other processes to 
reserve and process the job.
## How it works

### Monitoring Process Lifecycle
- The monitor process uses `process.monitor_process()` from `gleam/erlang/
  process` to track all queues and scheduled job 
  actors.
- If a queue or scheduled job process dies, the monitor receives a 
  process down signal.
- Upon receiving this signal, the monitor will release all 
  reservations made by that queue or job in the database. This makes 
  the job available for re-claiming.

### Periodic Job Reservation Cleanup (Not implemented yet)
- In cases where the monitor doesn't receive a process down signal 
  (e.g., if the signal is missed or delayed), it periodically loops 
  through reserved jobs in the database. 
- The monitor looks for jobs associated with dead processes, and 
  releases their reservation, allowing these jobs to be claimed again 
  by a new process. 
- The cleanup frequency is configurable, meaning you can define how 
  often the monitor checks for reserved jobs with dead processes. This 
  can be done using the configurable function some_fn().


### Handling Monitor Failures
- If the monitor itself dies, it’s designed to be fault-tolerant. 
  All queue and scheduled job processes are stored in an ETS (Erlang 
  Term Storage) table.
- Upon restart, the monitor performs a cleanup operation, where it 
  releases reservations for jobs that were associated with dead 
  processes.
- After cleanup, the monitor resumes its normal function by 
  restarting monitoring of the remaining alive processes.

---

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
30sec. the next run will be at 13:32 since it's only rescheduled 
after it completes.

# Schedules
Schedules for scheduled jobs can be specified in two ways. 
As an interval or as a cron-like schedule

Scheduled jobs are either executing or waiting to be executed. Since 
the job's next run is calculated on startup the first thing it does 
is wait.
However if there already is a scheduled job when the actor starts it 
will do nothing and wait for that job to become available.

## Intervals 
Intervals is the simplest for of schedules provided. Intervals can 
be specified in millieseconds, seconds, minutes, hours , days or 
weeks. This is done using the corresponding 
`scheduled_job.new_interval_[ time unit]()` functions, ex. 
`scheduled_job.new_interval_minutes(20)` would create a new interval 
of 20 minutes.
Execution time is not included in the interval. This means, what you're 
really setting, is how far in the future the job should be scheduled 
for. 

TODO: diagram

## Schedules
Sometimes you need to have more advanced schedules. For example you 
may want a job run on the first day of the month at 08:30, or
on thursdays between the months march and june.

This can be achieved using schedules. Schedules allows you to 
specify cron-like schedules ranges or specific values.

To achieve this flexibility, schedules are defined by configuring the 
following time components:
- Seconds: (Default: 0)
- Minutes: (Default: Every)
- Hours: (Default: Every)
- Day of the Month: (Default: Every)
- Month: (Default: Every)
- Day of the Week: (Default: Every)

Each component can be set to:
*Every:* Matches all possible values for that component.
*List:* A combination of:
 - Specific values (e.g., seconds 0, 15, 30).
 - Ranges (e.g., hours from 1 to 10).
Using List, you can mix specific values and ranges, allowing highly customized schedules.

### Behavior of Every
Setting a component to Every means it matches all possible values for that component. For example:
>Setting hours to Every will trigger the job at any hour, as long as other time units match their criteria.
---
### Schedule Validation

Schedules are validated when the bg_jobs framework is started. If a 
schedule is invalid, a `ScheduleValidationError` is raised, preventing 
the application from starting with an incorrect schedule configuration.

**Common Validation Errors**
An invalid schedule can occur if:
- A specified value is out of bounds for the time unit.
  - Example: Scheduling a job for the 13th month, which does not exist.
- A range is incorrectly defined (e.g., start is greater than end).
  - Example: A between_seconds(50, 30) call would trigger an error.

*Handling Validation Errors*
If the schedule is invalid a ScheduleValidationError is returned and 
will stop the creation of bg_jobs, returning said error. The error 
message will provide details about the issue.

---
### ScheduleBuilder Functions
The ScheduleBuilder provides a series of methods to configure each time component.
*Creating a New Schedule*
```gleam
pub fn new_schedule() -> ScheduleBuilder
```
Creates a new schedule with the default configuration: triggers at the first second of every minute.

---
### Configuring Seconds
*every_second*
```gleam
pub fn every_second(self: ScheduleBuilder) -> ScheduleBuilder
```
Sets the schedule to trigger at every second.

*on_second*
```gleam
pub fn on_second(self: ScheduleBuilder, second: Int) -> ScheduleBuilder
```
Adds a specific second to the schedule. If other seconds are already 
defined, this appends the new value.

*Example:*
```gleam
schedule.on_second(1).on_second(10);
// Triggers at second 1 or 10.
```

*between_seconds*
```gleam
pub fn between_seconds(self: ScheduleBuilder, start: Int, end: Int) -> ScheduleBuilder
```
Adds a range of seconds during which the schedule should trigger. If other seconds or ranges are already defined, this appends the range.


Example:
```gleam
schedule.on_second(1).between_seconds(10, 15);
// Triggers at second 1 or seconds 10–15.
```

---

### Combining Filters
Filters across time units are combined using AND logic. For example:
```gleam
let schedule = new_schedule()
    .on_second(1)
    .between_seconds(10, 15)
    .on_minute(30)
    .on_hour(8)
    .between_hours(14, 16)
    .on_thursdays()
    .between_months(3, 6);
```
This schedule will trigger only when:
- The second is 1 **or** within the range 10–15, **and**
- The minute is 30, **and**
- The hour is 8 **or** between 14:00–16:00, **and**
- It is Thursday, **and**
- It is during March through June.

--- 
### Practical Example
Here’s a complete example demonstrating both OR and AND logic:

```gleam
let schedule = new_schedule()
    .on_second(1)
    .on_second(10)
    .between_seconds(30, 40)
    .on_minute(15)
    .on_minute(45)
    .on_hour(3)
    .between_hours(10, 12)
    .on_mondays() 
    .on_januaries()
    .on_decembers();
```
This schedule will trigger only when:
- The second is 1, 10, **or** between 30–40, **and**
- The minute is 15 **or** 45, **and**
- The hour is 3 **or** between 10–12, **and**
- It is Monday, **and**
- It is January **or** December.

---
### Summary

The ScheduleBuilder provides a powerful and flexible way to define schedules:
- **OR logic within each time unit** allows combining multiple specific values and ranges.
- **AND logic across time units** ensures precise scheduling, requiring all units to match their criteria simultaneously. This design makes it easy to express even the most complex scheduling requirements.

---
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

If it's registered, this function will be called whenever an event 
occurs, receiving an Event object as its parameter. You can  
register your custom listener with bg_jobs (global listener), a queue,
or a scheduled job as needed.

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
