import bg_jobs/errors
import bg_jobs/internal/events
import birl
import gleam/string
import logging.{Info}

/// Listen to events and logg them using the [logging](https://hexdocs.pm/logging/) library
///
pub fn listner(event: events.Event) {
  let now = birl.now() |> birl.to_iso8601()
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
        logging.Debug,
        now
          <> "|DbQueryEvent|sql:"
          <> sql
          <> "|attributes:"
          <> string.inspect(attributes),
      )
    events.DbResponseEvent(response) ->
      logging.log(
        logging.Debug,
        now <> "|DbResponseEvent|response:" <> response,
      )
    events.DbErrorEvent(error) ->
      logging.log(
        logging.Error,
        now <> "|DbErrorEvent|response:" <> error_to_string(error),
      )
    events.SetupErrorEvent(error) ->
      logging.log(
        logging.Error,
        now <> "|SetupErrorEvent|error: " <> string.inspect(error),
      )
    events.DbEvent(event, input) ->
      logging.log(
        logging.Error,
        now
          <> "|DbEvent|"
          <> event
          <> "|input:"
          <> string.join(input, with: ","),
      )
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
  }
}
