import bg_jobs
import bg_jobs/errors
import bg_jobs/events
import bg_jobs/internal/utils
import gleam/list
import gleam/result
import gleam/string
import logging.{Debug, Error, Info}
import tempo/naive_datetime

/// Listen to events and logg them using the [logging](https://hexdocs.pm/logging/) library
///
pub fn listner(event: events.Event) {
  case event {
    events.JobEnqueuedEvent(job) ->
      logging.log(Info, "JobEnqueued|job_name:" <> job.name)
    events.JobFailedEvent(queue_name, job) ->
      logging.log(
        Info,
        "JobFailed|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    events.JobReservedEvent(queue_name, job) ->
      logging.log(
        Info,
        "JobReserved|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    events.JobStartEvent(queue_name, job) ->
      logging.log(
        Info,
        "JobStart|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    events.JobSucceededEvent(queue_name, job) ->
      logging.log(
        Info,
        "JobSucceeded|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    events.QueueErrorEvent(queue_name, error) ->
      logging.log(
        logging.Error,
        "QueueError|queue_name:"
          <> queue_name
          <> "|error:"
          <> error_to_string(error),
      )
    events.QueuePollingStartedEvent(queue_name) ->
      logging.log(Info, "QueuePollingStarted|queue_name:" <> queue_name)
    events.QueuePollingStopedEvent(queue_name) ->
      logging.log(Info, "QueuePollingStoped|queue_name:" <> queue_name)
    events.DbQueryEvent(sql, attributes) ->
      logging.log(
        Debug,
        "DbQueryEvent|sql:"
          <> sql
          <> "|attributes:"
          <> string.inspect(attributes),
      )
    events.DbResponseEvent(response) ->
      logging.log(Debug, "DbResponseEvent|response:" <> response)
    events.DbErrorEvent(error) ->
      logging.log(Error, "DbErrorEvent|response:" <> error_to_string(error))
    events.SetupErrorEvent(error) ->
      logging.log(Error, "SetupErrorEvent|error: " <> string.inspect(error))
    events.DbEvent(event, input) ->
      logging.log(
        Debug,
        "DbEvent|" <> event <> "|input:" <> string.join(input, with: ","),
      )
    events.MigrateDownComplete -> logging.log(Info, "MigrateDownComplete")
    events.MigrateUpComplete -> logging.log(Info, "MigrateUpComplete")
    events.MonitorReleasingReserved(pid) ->
      logging.log(Info, "MonitorReleasingReserved" <> utils.pid_to_string(pid))
    events.MonitorReleasedJob(job) ->
      logging.log(Info, "MonitorReleasedJob" <> job.id)
    events.NoWorkerForJobError(job_request) ->
      logging.log(
        Error,
        "NoWorkerForJobError|name:"
          <> job_request.name
          <> "|payload:"
          <> job_request.payload
          <> "|available_at:"
          <> {
          bg_jobs.available_at_from_availability(job_request.availability)
          |> utils.from_tuple()
          |> result.map(naive_datetime.to_string)
          |> result.unwrap("")
        },
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
    errors.DateOutOfBoundsError(err) ->
      "DateOutOfBoundsError|error" <> string.inspect(err)
    errors.TimeOutOfBoundsError(err) ->
      "TimeOutOfBoundsError|error" <> string.inspect(err)
    errors.NoWorkerForJobError(job_request, workers) ->
      "NoWorkerForJob|job_request:"
      <> string.inspect(job_request)
      <> "|workers:"
      <> list.map(workers, fn(w) { w.job_name }) |> string.join(with: "")
    errors.ScheduleValidationError(e) -> "ScheduleValidationError" <> e
  }
}
