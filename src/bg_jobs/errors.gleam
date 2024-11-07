import gleam/otp/actor

/// Represents errors that can occur during background job processing.
pub type BgJobError {
  DbError(String)
  ParseDateError(String)
  DispatchJobError(String)
  SetupError(actor.StartError)
  ScheduleError(String)
  UnknownError(String)
}
