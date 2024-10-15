import bg_jobs
import birl
import gleam/string
import logging.{Info}

pub fn listner(event: bg_jobs.Event) {
  let now = birl.now() |> birl.to_iso8601()
  case event {
    bg_jobs.JobEnqueuedEvent(job) ->
      logging.log(Info, now <> "|JobEnqueued|job_name:" <> job.name)
    bg_jobs.JobFailedEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobFailed|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    bg_jobs.JobReservedEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobReserved|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    bg_jobs.JobStartEvent(queue_name, job) ->
      logging.log(
        Info,
        now <> "|JobStart|queue_name:" <> queue_name <> "|job_name:" <> job.name,
      )
    bg_jobs.JobSuccededEvent(queue_name, job) ->
      logging.log(
        Info,
        now
          <> "|JobSucceded|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> job.name,
      )
    bg_jobs.QueueErrorEvent(queue_name, error) ->
      logging.log(
        logging.Error,
        now
          <> "|QueueError|queue_name:"
          <> queue_name
          <> "|job_name:"
          <> error_to_string(error),
      )
    bg_jobs.QueuePollingStartedEvent(queue_name) ->
      logging.log(Info, now <> "|QueuePollingStarted|queue_name:" <> queue_name)
    bg_jobs.QueuePollingStopedEvent(queue_name) ->
      logging.log(Info, now <> "|QueuePollingStoped|queue_name:" <> queue_name)
    bg_jobs.DbQueryEvent(sql, attributes) ->
      logging.log(
        logging.Debug,
        now
          <> "|DbQueryEvent|sql:"
          <> sql
          <> "|attributes:"
          <> string.inspect(attributes),
      )
    bg_jobs.DbResponseEvent(response) ->
      logging.log(
        logging.Debug,
        now <> "|DbResponseEvent|response:" <> response,
      )
    bg_jobs.DbErrorEvent(error) ->
      logging.log(
        logging.Error,
        now <> "|DbErrorEvent|response:" <> error_to_string(error),
      )
  }
  Nil
}

fn error_to_string(error: bg_jobs.BgJobError) {
  case error {
    bg_jobs.DbError(_err) -> "DbError|message: Database error"
    bg_jobs.DispatchJobError(reason) -> "DispatchJobError|message:" <> reason
    bg_jobs.ParseDateError(reason) -> "ParseDateError|message:" <> reason
    bg_jobs.SetupError(_) -> "SetupError|message:Could not start actor"
    bg_jobs.UnknownError(reason) -> "UnknownError|message:" <> reason
  }
}
