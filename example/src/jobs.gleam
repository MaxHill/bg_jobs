import bg_jobs
import bg_jobs/queue
import bg_jobs/sqlite_store
import gleam/erlang/process
import gleam/list
import gleam/otp/supervisor
import jobs/send_email
import sqlight

pub type Queues(msg) {
  DefaultQueue(process.Subject(queue.Message(msg)))
  SecondQueue(process.Subject(queue.Message(msg)))
}

pub fn setup_queues(conn: sqlight.Connection) {
  let assert Ok(_) = bg_jobs.migrate_up(conn)

  let job_store = sqlite_store.try_new_store(conn)
  let assert Ok(registry) = queue.registry()

  let default_queue =
    queue.new_otp_worker(
      registry: registry,
      queue_type: DefaultQueue,
      max_retries: 3,
      job_store: job_store,
      job_mapper: queue.match_worker([send_email.lookup]),
    )
  let second_queue =
    queue.new_otp_worker(
      registry: registry,
      queue_type: SecondQueue,
      max_retries: 3,
      job_store: job_store,
      job_mapper: queue.match_worker([]),
    )

  let assert Ok(sup) =
    supervisor.start_spec(
      supervisor.Spec(
        argument: Nil,
        frequency_period: 1,
        max_frequency: 5,
        init: fn(children) {
          [default_queue, second_queue]
          |> list.fold(children, fn(c, q) { supervisor.add(c, q) })
        },
      ),
    )

  #(registry, sup)
}

pub fn dispatch_send_email(queues, payload: send_email.SendEmailPayload) {
  send_email.dispatch(default_queue(queues), payload)
}

pub fn default_queue(queues) {
  let assert DefaultQueue(queue) = queue.get_queue(queues, DefaultQueue)
  queue
}
