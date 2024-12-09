import bg_jobs/jobs
import gleam/dynamic
import tempo

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(String)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(dynamic.Dynamic)
  ScheduleError(String)
  UnknownError(String)
  NoWorkerForJobError(jobs.JobEnqueueRequest, List(jobs.Worker))
  DateOutOfBoundsError(tempo.DateOutOfBoundsError)
  TimeOutOfBoundsError(tempo.TimeOutOfBoundsError)
}
