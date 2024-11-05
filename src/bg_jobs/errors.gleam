import gleam/otp/actor
import sqlight

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(sqlight.Error)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(actor.StartError)
  ScheduleError(String)
  UnknownError(String)
}
