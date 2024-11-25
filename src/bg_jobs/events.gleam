import bg_jobs/errors
import bg_jobs/jobs
import gleam/list
import gleam/otp/actor

// Event
//---------------
pub type Event {
  SetupErrorEvent(message: actor.StartError)
  JobEnqueuedEvent(job: jobs.Job)
  JobReservedEvent(queue_name: String, job: jobs.Job)
  JobStartEvent(queue_name: String, job: jobs.Job)
  JobSucceededEvent(queue_name: String, job: jobs.Job)
  JobFailedEvent(queue_name: String, job: jobs.Job)
  QueuePollingStartedEvent(queue_name: String)
  QueuePollingStopedEvent(queue_name: String)
  QueueErrorEvent(queue_name: String, error: errors.BgJobError)
  DbQueryEvent(sql: String, attributes: List(String))
  DbEvent(operation: String, input: List(String))
  DbResponseEvent(response: String)
  DbErrorEvent(error: errors.BgJobError)

  MigrateUpComplete
  MigrateDownComplete
}

pub type EventListener =
  fn(Event) -> Nil

pub fn send_event(event_listners: List(EventListener), event: Event) {
  event_listners
  |> list.each(fn(handler) { handler(event) })
}
