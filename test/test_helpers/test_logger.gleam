import gleam/erlang/process
import gleam/list
import gleam/otp/actor

pub fn new_logger() {
  let assert Ok(actor) = actor.start(list.new(), handle_log_message)
  actor
}

pub fn log_message(log: process.Subject(LogMessage)) {
  fn(log_line: String) { actor.send(log, LogMessage(log_line)) }
}

pub fn get_log(log: process.Subject(LogMessage)) {
  actor.call(log, GetLog, 100)
}

pub type LogMessage {
  LogMessage(String)
  GetLog(reply_with: process.Subject(List(String)))
}

fn handle_log_message(
  message: LogMessage,
  log: List(String),
) -> actor.Next(LogMessage, List(String)) {
  case message {
    LogMessage(log_line) -> actor.continue(list.append(log, [log_line]))
    GetLog(client) -> {
      process.send(client, log)
      actor.continue(log)
    }
  }
}
