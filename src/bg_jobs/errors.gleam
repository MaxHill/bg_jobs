import gleam/otp/actor
import tempo

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(String)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(actor.StartError)
  ScheduleError(String)
  UnknownError(String)
  DateOutOfBoundsError(tempo.DateOutOfBoundsError)
  TimeOutOfBoundsError(tempo.TimeOutOfBoundsError)
}
