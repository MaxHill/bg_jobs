import bg_jobs/internal/messages
import bg_jobs/internal/scheduled_jobs_messages
import chip

pub type QueueRegistry =
  chip.Registry(messages.QueueMessage, String, Nil)

pub type DispatcherRegistry =
  chip.Registry(messages.DispatcherMessage, String, Nil)

pub type ScheduledJobRegistry =
  chip.Registry(scheduled_jobs_messages.Message, String, Nil)

pub type Registries {
  Registries(
    queue_registry: QueueRegistry,
    dispatcher_registry: DispatcherRegistry,
    scheduled_jobs_registry: ScheduledJobRegistry,
  )
}
