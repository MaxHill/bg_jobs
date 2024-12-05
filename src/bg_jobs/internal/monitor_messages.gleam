import gleam/erlang/process

// Monitor
//---------------
pub type Message {
  Shutdown
  /// Register a new actor process for monitoring.
  ///
  /// Allows monitor to start tracking the actor,
  /// enabling lifecycle management and job reservation tracking.
  Register(process.Pid)
  /// Handle the termination of a monitored actor process.
  ///
  /// Triggers cleanup operations for jobs associated with the 
  /// terminated process.
  Down(process.ProcessDown)

  /// Proactively releases job reservations for processes that are no longer alive.
  ///
  /// This message serves as a fallback mechanism for job reservation cleanup. While 
  /// job reservations are typically released automatically when a process dies, this 
  /// message provides an additional safety net to handle scenarios where the standard 
  /// down message might have been missed or not processed.
  ///
  ReleaseAbandonedReservations
}
