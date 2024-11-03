import bg_jobs/internal/jobs

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
  Worker(job_name: String, handler: fn(jobs.Job) -> Result(Nil, String))
}
