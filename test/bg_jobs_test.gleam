import bg_jobs
import bg_jobs/internal/utils
import bg_jobs/sqlite_db_adapter
import chip
import gleam/erlang/process
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import gleeunit
import gleeunit/should
import sqlight
import test_helpers
import test_helpers/jobs
import test_helpers/jobs/failing_job
import test_helpers/jobs/log_job
import youid/uuid

pub fn main() {
  gleeunit.main()
}

pub fn single_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, logger, _event_logger) <- jobs.setup(conn)

  let assert Ok(_job) = log_job.dispatch(bg, log_job.Payload("test message"))

  // Wait for jobs to process
  process.sleep(200)

  test_helpers.get_log(logger)
  |> should.equal(["test message"])
}

pub fn job_is_moved_to_success_after_succeeding_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, _logger, _event_logger) <- jobs.setup(conn)

  let assert Ok(_job) = log_job.dispatch(bg, log_job.Payload("test message 1"))

  // Wait for jobs to process
  process.sleep(50)

  db_adapter.get_succeeded_jobs(10)
  |> result.map(list.map(_, fn(job: bg_jobs.SucceededJob) {
    #(job.name, job.payload, job.attempts)
  }))
  |> should.be_ok
  |> should.equal([#("LOG_JOB", "\"test message 1\"", 0)])
}

pub fn process_muliptle_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, logger, _event_logger) <- jobs.setup(conn)
  let dispatch = log_job.dispatch(bg, _)

  let assert Ok(_) = dispatch(log_job.Payload("test message 1"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 2"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 3"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 4"))
  let assert Ok(_) = dispatch(log_job.Payload("test message 5"))

  // Wait for jobs to process
  process.sleep(300)

  test_helpers.get_log(logger)
  |> should.equal([
    "test message 1", "test message 2", "test message 3", "test message 4",
    "test message 5",
  ])
}

pub fn failing_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, logger, _event_logger) <- jobs.setup(conn)

  let assert Ok(_job) =
    failing_job.dispatch(bg, failing_job.FailingPayload("Failing"))

  // Wait for jobs to process
  process.sleep(100)

  db_adapter.get_failed_jobs(1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(failed_job) {
    failed_job.attempts
    |> should.equal(3)
  }

  test_helpers.get_log(logger)
  |> should.equal([
    "Attempt: 0 - Failed with payload: Failing",
    "Attempt: 1 - Failed with payload: Failing",
    "Attempt: 2 - Failed with payload: Failing",
    "Attempt: 3 - Failed with payload: Failing",
  ])
}

pub fn handle_no_worker_found_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, db_adapter, logger, _event_logger) <- jobs.setup(conn)

  let assert Error(_) =
    bg_jobs.enqueue_job(
      bg_jobs.JobEnqueueRequest(
        "DOES_NOT_EXIST",
        json.to_string(json.string("payload")),
        bg_jobs.AvailableNow,
      ),
      bg,
    )

  let assert Ok(_) = log_job.dispatch(bg, log_job.Payload("testing"))

  // Wait for jobs to process
  process.sleep(50)

  // Make sure one of the jobs worked
  test_helpers.get_log(logger)
  |> should.equal(["testing"])

  // Make sure the failed job is available
  db_adapter.get_failed_jobs(1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(job) {
    job.name
    |> should.equal("DOES_NOT_EXIST")

    job.exception
    |> should.equal("Could not enqueue job with no worker")
  }
}

pub fn keep_going_after_panic_test() {
  use conn <- sqlight.with_connection(":memory:")
  let bad_adapter =
    bg_jobs.DbAdapter(
      ..sqlite_db_adapter.try_new_store(conn, []),
      claim_jobs: fn(_, _, _) { panic as "test panic" },
    )
  let assert Ok(_) = bad_adapter.migrate_down()
  let assert Ok(_) = bad_adapter.migrate_up()
  let logger = test_helpers.new_logger()

  let assert Ok(bg) =
    bg_jobs.new(bad_adapter)
    |> bg_jobs.add_queue(
      bg_jobs.new_queue("default_queue")
      |> bg_jobs.queue_add_worker(log_job.worker(logger)),
    )
    |> bg_jobs.create()

  let assert Ok(queue) = chip.find(bg.queue_registry, "default_queue")
  let assert Ok(_job) = log_job.dispatch(bg, log_job.Payload("test message"))

  // Wait for restart
  bg_jobs.start_processing_all(bg)
  process.sleep(300)

  let assert Ok(restarted_queue) = chip.find(bg.queue_registry, "default_queue")

  restarted_queue
  |> should.not_equal(queue)

  bg_jobs.stop_processing_all(bg)
  // Give it time to stop polling before connection closes
  process.sleep(100)
}

pub fn polling_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, logger, _event_logger) <- jobs.setup(conn)

  let assert Ok(_job) = log_job.dispatch(bg, log_job.Payload("test message"))

  // Wait for jobs to process
  process.sleep(50)

  test_helpers.get_log(logger)
  |> should.equal(["test message"])
}

pub fn events_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, _logger, event_logger) <- jobs.setup(conn)

  let assert Ok(_job) = log_job.dispatch(bg, log_job.Payload("test message"))

  // For logging order let the first event run it's course before
  // starting the next 
  process.sleep(200)

  let assert Ok(_job) =
    failing_job.dispatch(bg, failing_job.FailingPayload("test message"))

  // For logging order let the first event run it's course before
  // starting the next 
  process.sleep(200)

  let assert Error(_) =
    bg_jobs.enqueue_job(
      bg_jobs.JobEnqueueRequest(
        "DOES_NOT_EXIST",
        json.to_string(json.string("payload")),
        bg_jobs.AvailableNow,
      ),
      bg,
    )

  process.sleep(200)

  test_helpers.get_log(event_logger)
  |> list.sort(by: string.compare)
  |> should.equal(list.sort(
    [
      "Event:QueuePollingStarted|queue_name:default_queue",
      "Event:QueuePollingStarted|queue_name:second_queue",
      "Event:JobEnqueued|job_name:LOG_JOB",
      "Event:JobReserved|queue_name:default_queue|job_name:LOG_JOB",
      "Event:JobStart|queue_name:default_queue|job_name:LOG_JOB",
      "Event:JobSucceded|queue_name:default_queue|job_name:LOG_JOB",
      "Event:JobEnqueued|job_name:FAILING_JOB",
      "Event:JobReserved|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:JobStart|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:JobStart|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:JobStart|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:JobStart|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:JobFailed|queue_name:default_queue|job_name:FAILING_JOB",
      "Event:QueueError|queue_name:job_dispatcher|job_name:DbError|message:No worker for job: DOES_NOT_EXIST",
      "Event:JobFailed|queue_name:job_dispatcher|job_name:DOES_NOT_EXIST",
    ],
    by: string.compare,
  ))
}

pub fn handle_abandoned_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, logger, _event_logger) <- jobs.setup(conn)

  // This job is abandoned since it has reserved_at & _by. 
  // It will not be picked up by any queue
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO jobs (id, name, payload, attempts, created_at, available_at, reserved_at, reserved_by)
      VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, '2023-01-01 00:00:00', ?)
      RETURNING *;",
      conn,
      [
        sqlight.text(uuid.v4_string()),
        sqlight.text(log_job.job_name),
        sqlight.text(log_job.to_string(log_job.Payload("test"))),
        sqlight.text("default_queue"),
      ],
      utils.discard_decode,
    )

  let assert Ok(queue) = chip.find(bg.queue_registry, "default_queue")

  process.kill(process.subject_owner(queue))

  // Wait for kill to happen
  process.sleep(100)

  let assert Ok(new_queue) = chip.find(bg.queue_registry, "default_queue")
  new_queue |> should.not_equal(queue)

  // Give time to pick it up
  process.sleep(1000)

  test_helpers.get_log(logger)
  |> should.equal(["test"])
}

pub fn scheduled_job_test() {
  use conn <- sqlight.with_connection(":memory:")
  use #(bg, _db_adapter, logger, _event_logger) <- jobs.setup(conn)

  // Dispatch log job and make it available in the future
  let assert Ok(_) =
    bg_jobs.enqueue_job(
      bg_jobs.JobEnqueueRequest(
        log_job.job_name,
        log_job.to_string(log_job.Payload("test")),
        bg_jobs.AvailableIn(1000),
      ),
      bg,
    )

  process.sleep(100)

  test_helpers.get_log(logger)
  |> should.equal([])

  // Wait for test to come available and execute
  process.sleep(1000)

  test_helpers.get_log(logger)
  |> should.equal(["test"])
}

pub fn builder_test() {
  use conn <- sqlight.with_connection(":memory:")
  let db_adapter = sqlite_db_adapter.try_new_store(conn, [])
  let test_event_listener = fn(_: bg_jobs.Event) { Nil }
  let test_queue_spec =
    bg_jobs.QueueSpec(
      name: "test_queue",
      workers: [],
      event_listeners: [],
      max_retries: 0,
      init_timeout: 0,
      max_concurrent_jobs: 0,
      poll_interval: 0,
    )

  let spec =
    bg_jobs.new(db_adapter)
    |> bg_jobs.with_supervisor_max_frequency(10)
    |> bg_jobs.with_supervisor_frequency_period(10)
    |> bg_jobs.add_event_listener(test_event_listener)
    |> bg_jobs.add_queue(test_queue_spec)

  let spec2 =
    bg_jobs.new(db_adapter)
    |> bg_jobs.add_event_listener(test_event_listener)
    |> bg_jobs.add_queue(test_queue_spec)
    |> bg_jobs.add_event_listener(test_event_listener)
    |> bg_jobs.add_queue(test_queue_spec)

  spec.max_frequency |> should.equal(10)
  spec.frequency_period |> should.equal(10)
  spec.db_adapter |> should.equal(db_adapter)
  spec.event_listeners |> should.equal([test_event_listener])
  spec.queues |> should.equal([test_queue_spec])

  spec2.event_listeners
  |> should.equal([test_event_listener, test_event_listener])
  spec2.queues |> should.equal([test_queue_spec, test_queue_spec])
}

pub fn build_queue_test() {
  let test_event_listener = fn(_: bg_jobs.Event) { Nil }
  let test_worker =
    bg_jobs.Worker(job_name: "test_worker", handler: fn(_) { Ok(Nil) })

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_with_max_retries(1)
  |> bg_jobs.queue_with_init_timeout(2)
  |> bg_jobs.queue_with_max_concurrent_jobs(3)
  |> bg_jobs.queue_with_poll_interval_ms(4)
  |> should.equal(
    bg_jobs.QueueSpec(
      name: "test_queue",
      workers: [],
      max_retries: 1,
      init_timeout: 2,
      max_concurrent_jobs: 3,
      poll_interval: 4,
      event_listeners: [],
    ),
  )

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_with_workers([test_worker, test_worker])
  |> fn(queue) { queue.workers }
  |> list.length
  |> should.equal(2)

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_add_worker(test_worker)
  |> bg_jobs.queue_add_worker(test_worker)
  |> fn(queue) { queue.workers }
  |> list.length
  |> should.equal(2)

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_with_event_listeners([
    test_event_listener,
    test_event_listener,
  ])
  |> fn(queue) { queue.event_listeners }
  |> list.length
  |> should.equal(2)

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_add_event_listener(test_event_listener)
  |> bg_jobs.queue_add_event_listener(test_event_listener)
  |> fn(queue) { queue.event_listeners }
  |> list.length
  |> should.equal(2)

  bg_jobs.new_queue("test_queue")
  |> bg_jobs.queue_add_event_listener(test_event_listener)
  |> bg_jobs.queue_add_event_listeners([
    test_event_listener,
    test_event_listener,
  ])
  |> fn(queue) { queue.event_listeners }
  |> list.length
  |> should.equal(3)
}
