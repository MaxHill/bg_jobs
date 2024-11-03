import bg_jobs/internal/errors
import bg_jobs/internal/jobs
import gleam/list

// Event
//---------------
pub type Event {
  JobEnqueuedEvent(job: jobs.Job)
  JobReservedEvent(queue_name: String, job: jobs.Job)
  JobStartEvent(queue_name: String, job: jobs.Job)
  JobSuccededEvent(queue_name: String, job: jobs.Job)
  JobFailedEvent(queue_name: String, job: jobs.Job)
  QueuePollingStartedEvent(queue_name: String)
  QueuePollingStopedEvent(queue_name: String)
  QueueErrorEvent(queue_name: String, error: errors.BgJobError)
  DbQueryEvent(sql: String, attributes: List(String))
  DbResponseEvent(response: String)
  DbErrorEvent(error: errors.BgJobError)
}

pub type EventListener =
  fn(Event) -> Nil

pub fn send_event(event_listners: List(EventListener), event: Event) {
  event_listners
  |> list.each(fn(handler) { handler(event) })
}
