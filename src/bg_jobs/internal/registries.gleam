import bg_jobs/internal/dispatcher_messages
import bg_jobs/internal/monitor_messages
import bg_jobs/internal/queue_messages
import bg_jobs/internal/scheduled_jobs_messages
import chip

pub type QueueRegistry =
  chip.Registry(queue_messages.Message, String, Nil)

pub type DispatcherRegistry =
  chip.Registry(dispatcher_messages.Message, String, Nil)

pub type ScheduledJobRegistry =
  chip.Registry(scheduled_jobs_messages.Message, String, Nil)

pub type MonitorRegistry =
  chip.Registry(monitor_messages.Message, String, Nil)

pub type Registries {
  Registries(
    queue_registry: QueueRegistry,
    dispatcher_registry: DispatcherRegistry,
    scheduled_jobs_registry: ScheduledJobRegistry,
    monitor_registry: MonitorRegistry,
  )
}
