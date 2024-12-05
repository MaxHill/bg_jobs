import bg_jobs/errors
import bg_jobs/events
import bg_jobs/jobs

// Database
//---------------

/// Defines the interface for database interactions related to job management.
///
/// Each database implementation must provide functions to enqueue, retrieve, and manage jobs.
///
pub type DbAdapter {
  DbAdapter(
    enqueue_job: fn(String, String, #(#(Int, Int, Int), #(Int, Int, Int))) ->
      Result(jobs.Job, errors.BgJobError),
    reserve_jobs: fn(List(String), Int, String) ->
      Result(List(jobs.Job), errors.BgJobError),
    release_reservation: fn(String) -> Result(jobs.Job, errors.BgJobError),
    release_jobs_reserved_by: fn(String) ->
      Result(List(jobs.Job), errors.BgJobError),
    move_job_to_succeeded: fn(jobs.Job) -> Result(Nil, errors.BgJobError),
    move_job_to_failed: fn(jobs.Job, String) -> Result(Nil, errors.BgJobError),
    increment_attempts: fn(jobs.Job) -> Result(jobs.Job, errors.BgJobError),
    get_enqueued_jobs: fn(String) -> Result(List(jobs.Job), errors.BgJobError),
    get_running_jobs: fn() -> Result(List(jobs.Job), errors.BgJobError),
    get_running_jobs_by_queue_name: fn(String) ->
      Result(List(jobs.Job), errors.BgJobError),
    get_succeeded_jobs: fn(Int) ->
      Result(List(jobs.SucceededJob), errors.BgJobError),
    get_failed_jobs: fn(Int) -> Result(List(jobs.FailedJob), errors.BgJobError),
    migrate_up: fn(List(events.EventListener)) -> Result(Nil, errors.BgJobError),
    migrate_down: fn(List(events.EventListener)) ->
      Result(Nil, errors.BgJobError),
  )
}
