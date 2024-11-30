import bg_jobs/internal/utils
import bg_jobs/jobs
import bg_jobs/sqlite_db_adapter
import gleam/dynamic
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/order
import gleeunit/should
import sqlight
import tempo/duration
import tempo/naive_datetime
import test_helpers

const job_name = "test-job"

const job_payload = "test-payload"

pub fn enqueue_job_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(returned_job) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  process.sleep(100)

  let assert Ok(jobs) =
    sqlight.query(
      "SELECT * FROM jobs",
      conn,
      [],
      sqlite_db_adapter.decode_enqueued_db_row,
    )

  list.length(jobs)
  |> should.equal(1)

  list.first(jobs)
  |> should.be_ok
  |> fn(job) {
    should.equal(returned_job, job)

    validate_job(job, job_name, job_payload)

    job.reserved_at
    |> should.be_none
  }
}

pub fn claim_jobs_limit_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_returned_job1) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  let assert Ok(_returned_job2) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  process.sleep(1000)
  sqlight.query(
    "select * from jobs",
    conn,
    [],
    sqlite_db_adapter.decode_enqueued_db_row,
  )
  |> should.be_ok
  |> list.length
  |> should.equal(2)

  job_store.claim_jobs([job_name], 1, "default_queue")
  |> should.be_ok
  |> list.length
  |> should.equal(1)

  job_store.claim_jobs([job_name], 1, "default_queue")
  |> should.be_ok
  |> list.length
  |> should.equal(1)

  job_store.claim_jobs([job_name], 1, "default_queue")
  |> should.be_ok
  |> list.length
  |> should.equal(0)
}

pub fn claim_jobs_returned_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  job_store.claim_jobs([job_name], 1, "default_queue")
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(job) {
    validate_job(job, job_name, job_payload)

    job.reserved_at
    |> should.be_some

    job.reserved_by
    |> should.be_some
    |> should.equal("default_queue")
  }
}

pub fn move_job_to_success_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(job) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  job_store.move_job_to_succeeded(job)
  |> should.be_ok

  sqlight.query(
    "SELECT * FROM jobs",
    conn,
    [],
    sqlite_db_adapter.decode_enqueued_db_row,
  )
  |> should.be_ok()
  |> list.length
  |> should.equal(0)

  sqlight.query(
    "SELECT * FROM jobs_succeeded",
    conn,
    [],
    sqlite_db_adapter.decode_succeeded_db_row,
  )
  |> should.be_ok()
  |> list.first
  |> should.be_ok
  |> should.equal(jobs.SucceededJob(
    id: job.id,
    name: job.name,
    payload: job.payload,
    attempts: job.attempts,
    created_at: job.created_at,
    available_at: job.available_at,
    succeeded_at: naive_datetime.now_utc() |> naive_datetime.to_tuple(),
  ))
}

pub fn move_job_to_failed_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(job) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  job_store.move_job_to_failed(job, "test exception")
  |> should.be_ok

  sqlight.query(
    "SELECT * FROM jobs",
    conn,
    [],
    sqlite_db_adapter.decode_enqueued_db_row,
  )
  |> should.be_ok()
  |> list.length
  |> should.equal(0)

  sqlight.query(
    "SELECT * FROM jobs_failed",
    conn,
    [],
    sqlite_db_adapter.decode_failed_db_row,
  )
  |> should.be_ok()
  |> list.first
  |> should.be_ok
  |> should.equal(jobs.FailedJob(
    id: job.id,
    name: job.name,
    payload: job.payload,
    attempts: job.attempts,
    exception: "test exception",
    created_at: job.created_at,
    available_at: job.available_at,
    failed_at: naive_datetime.now_utc() |> naive_datetime.to_tuple(),
  ))
}

pub fn get_succeeded_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs_succeeded (
      id, 
      name, 
      payload, 
      attempts, 
      created_at, 
      available_at, 
      succeeded_at
    )
    VALUES (
      'job_12345',                                 
      'process_order',                             
      '\"test-payload\"',       
      3,                                           
      '2024-09-29 10:30:00',                       
      '2024-09-29 10:30:00',                        
      '2024-09-29 11:00:00'                        
    );
    ",
      conn,
    )

  job_store.get_succeeded_jobs(1)
  |> should.be_ok
  |> should.equal([
    jobs.SucceededJob(
      "job_12345",
      "process_order",
      "\"test-payload\"",
      3,
      #(#(2024, 09, 29), #(10, 30, 00)),
      #(#(2024, 09, 29), #(10, 30, 00)),
      #(#(2024, 09, 29), #(11, 00, 00)),
    ),
  ])
}

pub fn get_failed_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs_failed (
      id, 
      name, 
      payload, 
      attempts, 
      exception,
      created_at, 
      available_at, 
      failed_at
    )
    VALUES (
      'job_12345',                                 
      'process_order',                             
      '\"test-payload\"',       
      3,                                           
      'Test exception',
      '2024-09-29 10:30:00',                       
      '2024-09-29 10:30:00',
      '2024-09-30 11:00:00'                        
    );
    ",
      conn,
    )

  job_store.get_failed_jobs(1)
  |> should.be_ok
  |> should.equal([
    jobs.FailedJob(
      "job_12345",
      "process_order",
      "\"test-payload\"",
      3,
      "Test exception",
      #(#(2024, 9, 29), #(10, 30, 0)),
      #(#(2024, 9, 29), #(10, 30, 0)),
      #(#(2024, 9, 30), #(11, 0, 0)),
    ),
  ])
}

pub fn increment_attempts_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(job) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  job_store.increment_attempts(job)
  |> should.be_ok
  |> fn(j: jobs.Job) { j.attempts }
  |> should.equal(1)
}

pub fn migrate_test() {
  use conn <- sqlight.with_connection(":memory:")
  let event_logger = test_helpers.new_logger()
  let logger_event_listener = test_helpers.new_logger_event_listner(
    event_logger,
    _,
  )

  let assert Ok(_) = sqlite_db_adapter.migrate_up(conn)([logger_event_listener])

  let sql =
    "
    SELECT name 
    FROM sqlite_master 
    WHERE type = 'table';"

  sqlight.query(
    sql,
    conn,
    [],
    dynamic.decode1(fn(str) { str }, dynamic.element(0, dynamic.string)),
  )
  |> should.be_ok
  |> should.equal(["jobs", "jobs_failed", "jobs_succeeded"])

  let assert Ok(_) =
    sqlite_db_adapter.migrate_down(conn)([logger_event_listener])
  sqlight.query(
    sql,
    conn,
    [],
    dynamic.decode1(fn(str) { str }, dynamic.element(0, dynamic.string)),
  )
  |> should.be_ok
  |> should.equal([])

  test_helpers.get_log(event_logger)
  |> should.equal(["Event:MigrateUpComplete", "Event:MigrateDownComplete"])
}

pub fn empty_list_of_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  job_store.claim_jobs(["job_name"], 3, "default_queue")
  |> should.be_ok
}

pub fn multiple_list_of_jobs_test() {
  use conn <- sqlight.with_connection(":memory:")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )
  let assert Ok(_) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )
  let assert Ok(_) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )
  let assert Ok(_) =
    job_store.enqueue_job(
      job_name,
      job_payload,
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  job_store.claim_jobs(["job_name"], 3, "default_queue")
  |> should.be_ok
}

pub fn db_events_test() {
  let event_logger = test_helpers.new_logger()
  use conn <- sqlight.with_connection(":memory:")
  let job_store =
    sqlite_db_adapter.new(conn, [
      test_helpers.new_logger_event_listner(event_logger, _),
    ])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    job_store.enqueue_job(
      "test_job_1",
      "test_payaload_1",
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )
  let assert Ok(_) =
    job_store.enqueue_job(
      "test_job_2",
      "test_payaload_2",
      naive_datetime.now_utc() |> naive_datetime.to_tuple(),
    )

  let job_1 =
    job_store.claim_jobs(["test_job_1"], 1, "default_queue")
    |> should.be_ok
    |> list.first
    |> should.be_ok

  let assert Ok(_) = job_store.increment_attempts(job_1)

  let assert Ok(_) = job_store.move_job_to_succeeded(job_1)
  let assert Ok(_) = job_store.get_succeeded_jobs(1)

  let job_2 =
    job_store.claim_jobs(["test_job_2"], 1, "default_queue")
    |> should.be_ok
    |> list.first
    |> should.be_ok

  let assert Ok(_) = job_store.move_job_to_failed(job_2, "test exception")
  let _ =
    job_store.get_failed_jobs(1)
    |> should.be_ok
    |> list.first
    |> should.be_ok

  // There is dynamic data that get's logged, so it's 
  // hard to check the exact output. Checking the number of 
  // lines logged should be enough for now.
  test_helpers.get_log(event_logger)
  |> list.length()
  |> should.equal(20)
}

pub fn release_claim_test() {
  let event_logger = test_helpers.new_logger()
  use conn <- sqlight.with_connection(":memory:")
  let job_store =
    sqlite_db_adapter.new(conn, [
      test_helpers.new_logger_event_listner(event_logger, _),
    ])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  // TODO: replace CURRENT_TIMESTAMP
  let assert Ok(_) =
    sqlight.query(
      "INSERT INTO jobs (id, name, payload, attempts, created_at, available_at, reserved_at, reserved_by)
      VALUES (?, ?, ?, 0, ?, ?, '2023-01-01 00:00:00', ?)
      RETURNING *;",
      conn,
      [
        sqlight.text("test_id"),
        sqlight.text("test_job"),
        sqlight.text("test"),
        sqlight.text(naive_datetime.now_utc() |> sqlite_db_adapter.to_db_date()),
        sqlight.text(naive_datetime.now_utc() |> sqlite_db_adapter.to_db_date()),
        sqlight.text("default_queue"),
      ],
      utils.discard_decode,
    )

  job_store.release_claim("test_id")
  |> should.be_ok

  process.sleep(100)

  job_store.claim_jobs(["test_job"], 1, "default_queue")
  |> should.be_ok()
  |> list.first
  |> should.be_ok
}

pub fn scheduled_job_test() {
  let event_logger = test_helpers.new_logger()
  use conn <- sqlight.with_connection(":memory:")
  let job_store =
    sqlite_db_adapter.new(conn, [
      test_helpers.new_logger_event_listner(event_logger, _),
    ])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    job_store.enqueue_job(
      "test_job",
      "test_payaload",
      naive_datetime.now_utc()
        |> naive_datetime.add(duration.seconds(2))
        |> naive_datetime.to_tuple(),
    )

  process.sleep(200)

  job_store.claim_jobs(["test_job"], 1, "default_queue")
  |> should.be_ok
  |> should.equal([])

  // Wait for it to become available
  process.sleep(2000)

  job_store.claim_jobs(["test_job"], 1, "default_queue")
  |> should.be_ok
  |> list.map(fn(job) { job.name })
  |> should.equal(["test_job"])
}

pub fn get_running_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs (
        id, 
        name, 
        payload, 
        attempts, 
        created_at, 
        available_at, 
        reserved_at,
        reserved_by
      )
      VALUES (
        'job_12345',                                 
        'process_order',                             
        '\"test-payload\"',       
        3,                                           
        '2024-09-29 10:30:00',                       
        '2024-09-29 10:30:00',                        
        '2024-09-29 11:00:00',                        
        'test_queue'
      ), (
        'job_6789',                                 
        'process_order',                             
        '\"test-payload\"',       
        3,                                           
        '2024-09-29 10:30:00',                       
        '2024-09-29 10:30:00',
        NULL,
        NULL
      );
    ",
      conn,
    )

  job_store.get_running_jobs_by_queue_name("test_queue")
  |> should.be_ok
  |> should.equal([
    jobs.Job(
      id: "job_12345",
      name: "process_order",
      payload: "\"test-payload\"",
      attempts: 3,
      created_at: #(#(2024, 09, 29), #(10, 30, 00)),
      available_at: #(#(2024, 09, 29), #(10, 30, 00)),
      reserved_at: option.Some(#(#(2024, 09, 29), #(11, 00, 00))),
      reserved_by: option.Some("test_queue"),
    ),
  ])
}

pub fn get_enqueued_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let job_store = sqlite_db_adapter.new(conn, [fn(_) { Nil }])
  let assert Ok(_) = job_store.migrate_down([])
  let assert Ok(_) = job_store.migrate_up([])

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs (
        id, 
        name, 
        payload, 
        attempts, 
        created_at, 
        available_at
      )
      VALUES (
        'job_12345',                                 
        'process_order',                             
        '\"test-payload\"',       
        3,                                           
        '2024-09-29 10:30:00',                       
        '2024-09-29 10:30:00'                       
      );
    ",
      conn,
    )

  job_store.get_enqueued_jobs("process_order")
  |> should.be_ok
  |> should.equal([
    jobs.Job(
      id: "job_12345",
      name: "process_order",
      payload: "\"test-payload\"",
      attempts: 3,
      created_at: #(#(2024, 09, 29), #(10, 30, 00)),
      available_at: #(#(2024, 09, 29), #(10, 30, 00)),
      reserved_at: option.None,
      reserved_by: option.None,
    ),
  ])
}

// Helpers
fn validate_job(job: jobs.Job, job_name: String, job_payload: String) {
  job.name
  |> should.equal(job_name)

  job.payload
  |> should.equal(job_payload)

  should.equal(job.created_at, job.available_at)

  utils.from_tuple(job.created_at)
  |> should.be_ok()
  |> naive_datetime.compare(
    naive_datetime.now_utc() |> naive_datetime.subtract(duration.seconds(3)),
    _,
  )
  |> should.equal(order.Lt)

  utils.from_tuple(job.available_at)
  |> should.be_ok()
  |> naive_datetime.compare(
    naive_datetime.now_utc() |> naive_datetime.subtract(duration.seconds(3)),
    _,
  )
  |> should.equal(order.Lt)
}
