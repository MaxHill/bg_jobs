import gleam/erlang/process

// Monitor
//---------------
pub type Message {
  Shutdown
  /// Registers an actor for monitoring
  Register(process.Pid)
  /// Cleanup claimed jobs when an actor dies
  Down(process.ProcessDown)
  /// Clears out claimed jobs where the down message has been missed
  /// This can happen if the monitor actor is down or message got 
  /// lost along the way
  ClearMissed
}
