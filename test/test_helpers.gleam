import bg_jobs/errors
import bg_jobs/events
import bg_jobs/sqlite_db_adapter
import gleam/erlang/process
import gleam/list
import gleam/otp/actor
import gleam/string
import sqlight

pub fn reset_db(connection: sqlight.Connection) {
  let assert Ok(_) = sqlite_db_adapter.migrate_down(connection)([])
  let assert Ok(_) = sqlite_db_adapter.migrate_up(connection)([])
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
  event: events.Event,
) {
  case event {
    events.JobEnqueuedEvent(job) -> "JobEnqueued|job_name:" <> job.name
    events.JobFailedEvent(queue_name, job) ->
      "JobFailed|queue_name:" <> queue_name <> "|job_name:" <> job.name
    events.JobReservedEvent(queue_name, job) ->
      "JobReserved|queue_name:" <> queue_name <> "|job_name:" <> job.name
    events.JobStartEvent(queue_name, job) ->
      "JobStart|queue_name:" <> queue_name <> "|job_name:" <> job.name
    events.JobSucceededEvent(queue_name, job) ->
      "JobSucceeded|queue_name:" <> queue_name <> "|job_name:" <> job.name
    events.QueueErrorEvent(queue_name, error) ->
      "QueueError|queue_name:"
      <> queue_name
      <> "|job_name:"
      <> error_to_string(error)
    events.QueuePollingStartedEvent(queue_name) ->
      "QueuePollingStarted|queue_name:" <> queue_name
    events.QueuePollingStopedEvent(queue_name) ->
      "QueuePollingStoped|queue_name:" <> queue_name
    events.DbQueryEvent(sql, attributes) ->
      "DbQueryEvent|sql:" <> sql <> "|attributes:" <> string.inspect(attributes)
    events.DbResponseEvent(response) -> "DbResponseEvent|response:" <> response
    events.DbErrorEvent(error) ->
      "DbErrorEvent|response:" <> string.inspect(error)
    events.SetupErrorEvent(error) -> "SetupErrorEvent" <> string.inspect(error)
    events.DbEvent(event, input) ->
      "DbEvent|" <> event <> "|input:" <> string.join(input, with: ",")
    events.MigrateDownComplete -> "MigrateDownComplete"
    events.MigrateUpComplete -> "MigrateUpComplete"
  }
  |> fn(str) { "Event:" <> str }
  |> log_message(logger)
  Nil
}

fn error_to_string(error: errors.BgJobError) {
  case error {
    errors.DbError(_err) -> "DbError|message: Database error"
    errors.DispatchJobError(reason) -> "DbError|message:" <> reason
    errors.ParseDateError(reason) -> "DbError|message:" <> reason
    errors.SetupError(_) -> "DbError|message:Could not start actor"
    errors.UnknownError(reason) -> "DbError|message:" <> reason
    errors.ScheduleError(reason) -> "ScheduleError|message:" <> reason
    errors.DateOutOfBoundsError(err) ->
      "DateOutOfBoundsError|message:" <> string.inspect(err)
    errors.TimeOutOfBoundsError(err) ->
      "TimeOutOfBoundsError|message" <> string.inspect(err)
  }
}
