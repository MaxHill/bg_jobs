import bg_jobs/queue
import bg_jobs/sqlite_store
import gleam/erlang/process
import gleam/list
import gleam/otp/supervisor
import gleam/result
import singularity
import sqlight
import test_helpers
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import test_helpers/test_logger

pub type Queues(msg) {
  DefaultQueue(process.Subject(queue.Message(msg)))
  SecondQueue(process.Subject(queue.Message(msg)))
}

pub type Jobs {
  LogJob(log_job.Payload)
  FailingJob(failing_job.FailingPayload)
}

pub fn dispatch(queues, payload) {
  case payload {
    LogJob(payload) -> {
      let assert DefaultQueue(queue) = queue.get_queue(queues, DefaultQueue)
      queue.enqueue_job(queue, log_job.job_name, log_job.to_string(payload))
    }
    FailingJob(payload) -> {
      let assert DefaultQueue(queue) = queue.get_queue(queues, DefaultQueue)
      queue.enqueue_job(
        queue,
        failing_job.job_name,
        failing_job.to_string(payload),
      )
    }
  }
}

pub fn setup(conn: sqlight.Connection) {
  use _ <- result.map(test_helpers.reset_db(conn))
  let job_store = sqlite_store.try_new_store(conn)
  let logger = test_logger.new_logger()

  let assert Ok(registry) = singularity.start()

  let default_queue =
    queue.new_otp_worker(
      registry: registry,
      queue_type: DefaultQueue,
      max_retries: 3,
      job_store: job_store,
      workers: [log_job.worker(logger), failing_job.worker(logger)],
    )

  let second_queue =
    queue.new_otp_worker(
      registry: registry,
      queue_type: SecondQueue,
      max_retries: 3,
      job_store: job_store,
      workers: [log_job.worker(logger), failing_job.worker(logger)],
    )

  let assert Ok(_sup) =
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

  #(registry, job_store, logger)
}
