import bg_jobs
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import gleam/list
import gleam/otp/actor
import gleam/string
import sqlight

pub fn reset_db(connection: sqlight.Connection) {
  let assert Ok(_) = sqlite_db_adapter.migrate_down(connection)()
  let assert Ok(_) = sqlite_db_adapter.migrate_up(connection)()
}

// Test Logger
//---------------
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

// Test Logger Event Listner
//---------------
pub fn new_logger_event_listner(
  logger: process.Subject(LogMessage),
  event: bg_jobs.Event,
) {
  case event {
    bg_jobs.JobEnqueuedEvent(job) -> "JobEnqueued|job_name:" <> job.name
    bg_jobs.JobFailedEvent(queue_name, job) ->
      "JobFailed|queue_name:" <> queue_name <> "|job_name:" <> job.name
    bg_jobs.JobReservedEvent(queue_name, job) ->
      "JobReserved|queue_name:" <> queue_name <> "|job_name:" <> job.name
    bg_jobs.JobStartEvent(queue_name, job) ->
      "JobStart|queue_name:" <> queue_name <> "|job_name:" <> job.name
    bg_jobs.JobSuccededEvent(queue_name, job) ->
      "JobSucceded|queue_name:" <> queue_name <> "|job_name:" <> job.name
    bg_jobs.QueueErrorEvent(queue_name, error) ->
      "QueueError|queue_name:"
      <> queue_name
      <> "|job_name:"
      <> error_to_string(error)
    bg_jobs.QueuePollingStartedEvent(queue_name) ->
      "QueuePollingStarted|queue_name:" <> queue_name
    bg_jobs.QueuePollingStopedEvent(queue_name) ->
      "QueuePollingStoped|queue_name:" <> queue_name
    bg_jobs.DbQueryEvent(sql, attributes) ->
      "DbQueryEvent|sql:" <> sql <> "|attributes:" <> string.inspect(attributes)
    bg_jobs.DbResponseEvent(response) -> "DbResponseEvent|response:" <> response
    bg_jobs.DbErrorEvent(error) ->
      "DbErrorEvent|response:" <> string.inspect(error)
  }
  |> fn(str) { "Event:" <> str }
  |> log_message(logger)
  Nil
}

fn error_to_string(error: bg_jobs.BgJobError) {
  case error {
    bg_jobs.DbError(_err) -> "DbError|message: Database error"
    bg_jobs.DispatchJobError(reason) -> "DbError|message:" <> reason
    bg_jobs.ParseDateError(reason) -> "DbError|message:" <> reason
    bg_jobs.SetupError(_) -> "DbError|message:Could not start actor"
    bg_jobs.UnknownError(reason) -> "DbError|message:" <> reason
  }
}
