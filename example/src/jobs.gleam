import bg_jobs
import bg_jobs/logger_event_listener
import bg_jobs/queue
import bg_jobs/scheduled_job
import bg_jobs/sqlite_db_adapter
import jobs/cleanup_db_job
import jobs/delete_expired_sessions_job
import jobs/send_email_job
import sqlight

pub fn setup(conn: sqlight.Connection) {
  let db_adapter = sqlite_db_adapter.new(conn, [])
  let assert Ok(_) = db_adapter.migrate_up()

  bg_jobs.new(db_adapter)
  |> bg_jobs.with_event_listener(logger_event_listener.listner)
  // Queues
  |> bg_jobs.with_queue(default_queue())
  |> bg_jobs.with_queue(second_queue())
  // Scheduled jobs
  |> bg_jobs.with_scheduled_job(scheduled_job.new(
    cleanup_db_job.worker(),
    scheduled_job.interval_minutes(1),
  ))
  |> bg_jobs.with_scheduled_job(scheduled_job.new(
    delete_expired_sessions_job.worker(),
    scheduled_job.Schedule(
      scheduled_job.new_schedule()
      |> scheduled_job.on_second(10),
    ),
  ))
  |> bg_jobs.build()
}

fn default_queue() {
  queue.new("default_queue")
  |> queue.with_worker(send_email_job.worker())
}

fn second_queue() {
  queue.new("second_queue")
}
