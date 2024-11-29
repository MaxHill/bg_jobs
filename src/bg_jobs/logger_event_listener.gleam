import bg_jobs/errors
import bg_jobs/events
import gleam/string
import logging.{Debug, Error, Info}
import tempo/naive_datetime

/// Listen to events and logg them using the [logging](https://hexdocs.pm/logging/) library
///
pub fn listner(event: events.Event) {
  let now = naive_datetime.now_utc() |> naive_datetime.to_string()
  case event {
    events.JobEnqueuedEvent(job) ->
      logging.log(Info, now <> "|JobEnqueued|job_name:" <> job.name)
    events.JobFailedEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobFailed|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    events.JobReservedEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobReserved|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    events.JobStartEvent(queue_name, job) ->
      logging.log(
        Info,
        now <> "|JobStart|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    events.JobSucceededEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobSucceeded|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    events.QueueErrorEvent(queue_name, error) ->
      logging.log(
        logging.Error,
        now
          <> "|QueueError|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> error_to_string(error),
      )
    events.QueuePollingStartedEvent(queue_name) ->
      logging.log(Info, now <> "|QueuePollingStarted|queue_name:" <> queue_name)
    events.QueuePollingStopedEvent(queue_name) ->
      logging.log(Info, now <> "|QueuePollingStoped|queue_name:" <> queue_name)
    events.DbQueryEvent(sql, attributes) ->
      logging.log(
        Debug,
        now
          <> "|DbQueryEvent|sql:"
          <> sql
          <> "|attributes:"
          <> string.inspect(attributes),
      )
    events.DbResponseEvent(response) ->
      logging.log(Debug, now <> "|DbResponseEvent|response:" <> response)
    events.DbErrorEvent(error) ->
      logging.log(
        Error,
        now <> "|DbErrorEvent|response:" <> error_to_string(error),
      )
    events.SetupErrorEvent(error) ->
      logging.log(
        Error,
        now <> "|SetupErrorEvent|error: " <> string.inspect(error),
      )
    events.DbEvent(event, input) ->
      logging.log(
        Error,
        now
          <> "|DbEvent|"
          <> event
          <> "|input:"
          <> string.join(input, with: ","),
      )
    events.MigrateDownComplete -> logging.log(Info, "MigrateDownComplete")
    events.MigrateUpComplete -> logging.log(Info, "MigrateUpComplete")
  }
  Nil
}

fn error_to_string(error: errors.BgJobError) {
  case error {
    errors.DbError(_err) -> "DbError|message: Database error"
    errors.DispatchJobError(reason) -> "DispatchJobError|message:" <> reason
    errors.ParseDateError(reason) -> "ParseDateError|message:" <> reason
    errors.SetupError(_) -> "SetupError|message:Could not start actor"
    errors.UnknownError(reason) -> "UnknownError|message:" <> reason
    errors.ScheduleError(reason) -> "ScheduleError|message:" <> reason
    errors.DateOutOfBoundsError(err) ->
      "DateOutOfBoundsError|error" <> string.inspect(err)
    errors.TimeOutOfBoundsError(err) ->
      "TimeOutOfBoundsError|error" <> string.inspect(err)
  }
}
