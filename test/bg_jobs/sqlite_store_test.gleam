import bg_jobs
import bg_jobs/job
import bg_jobs/sqlite_store
import birdie
import birl
import gleam/list
import gleam/order
import gleeunit/should
import pprint
import sqlight
import test_helpers

const queue_name = "test-queue"

const job_name = "test-job"

const job_payload = "test-payload"

pub fn enqueue_job_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = bg_jobs.migrate_down(conn)
  let assert Ok(_) = bg_jobs.migrate_up(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(returned_job) =
    job_store.enqueue_job(queue_name, job_name, job_payload)

  let assert Ok(jobs) =
    sqlight.query(
      "SELECT * FROM jobs",
      conn,
      [],
      sqlite_store.decode_enqueued_db_row,
    )

  list.length(jobs)
  |> should.equal(1)

  list.first(jobs)
  |> should.be_ok
  |> should.be_ok
  |> fn(job) {
    should.equal(returned_job, job)

    validate_job(job, queue_name, job_name, job_payload)

    job.reserved_at
    |> should.be_none
  }
}

pub fn get_next_jobs_limit_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = bg_jobs.migrate_down(conn)
  let assert Ok(_) = bg_jobs.migrate_up(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(_returned_job1) =
    job_store.enqueue_job(queue_name, job_name, job_payload)

  let assert Ok(_returned_job2) =
    job_store.enqueue_job(queue_name, job_name <> "2", job_payload)

  job_store.get_next_jobs(queue_name, 1)
  |> should.be_ok
  |> list.length
  |> should.equal(1)

  job_store.get_next_jobs(queue_name, 1)
  |> should.be_ok
  |> list.length
  |> should.equal(1)

  job_store.get_next_jobs(queue_name, 1)
  |> should.be_ok
  |> list.length
  |> should.equal(0)
}

pub fn get_next_jobs_returned_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = bg_jobs.migrate_down(conn)
  let assert Ok(_) = bg_jobs.migrate_up(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(_) = job_store.enqueue_job(queue_name, job_name, job_payload)

  job_store.get_next_jobs(queue_name, 1)
  |> should.be_ok
  |> list.first
  |> should.be_ok
  |> fn(job) {
    validate_job(job, queue_name, job_name, job_payload)

    job.reserved_at
    |> should.be_some
  }
}

pub fn move_job_to_success_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = test_helpers.reset_db(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(job) = job_store.enqueue_job(queue_name, job_name, job_payload)

  job_store.move_job_to_succeded(job)
  |> should.be_ok

  sqlight.query(
    "SELECT * FROM jobs",
    conn,
    [],
    sqlite_store.decode_enqueued_db_row,
  )
  |> should.be_ok()
  |> list.length
  |> should.equal(0)

  sqlight.query(
    "SELECT * FROM jobs_succeeded",
    conn,
    [],
    sqlite_store.decode_succeded_db_row,
  )
  |> should.be_ok()
  |> list.first
  |> should.be_ok
  |> should.be_ok
  |> should.equal(job.SucceededJob(
    id: job.id,
    queue: job.queue,
    name: job.name,
    payload: job.payload,
    attempts: job.attempts,
    created_at: job.created_at,
    available_at: job.available_at,
    succeded_at: birl.to_erlang_universal_datetime(birl.now()),
  ))
}

pub fn move_job_to_failed_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = test_helpers.reset_db(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(job) = job_store.enqueue_job(queue_name, job_name, job_payload)

  job_store.move_job_to_failed(job, "test exception")
  |> should.be_ok

  sqlight.query(
    "SELECT * FROM jobs",
    conn,
    [],
    sqlite_store.decode_enqueued_db_row,
  )
  |> should.be_ok()
  |> list.length
  |> should.equal(0)

  sqlight.query(
    "SELECT * FROM jobs_failed",
    conn,
    [],
    sqlite_store.decode_failed_db_row,
  )
  |> should.be_ok()
  |> list.first
  |> should.be_ok
  |> should.be_ok
  |> should.equal(job.FailedJob(
    id: job.id,
    queue: job.queue,
    name: job.name,
    payload: job.payload,
    attempts: job.attempts,
    exception: "test exception",
    created_at: job.created_at,
    available_at: job.available_at,
    failed_at: birl.to_erlang_universal_datetime(birl.now()),
  ))
}

pub fn get_succeeded_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = test_helpers.reset_db(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs_succeeded (
      id, 
      queue, 
      name, 
      payload, 
      attempts, 
      created_at, 
      available_at, 
      succeded_at
    )
    VALUES (
      'job_12345',                                 
      'default',                                   
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
  |> pprint.format
  |> birdie.snap(title: "Get succeeded job from db")
}

pub fn get_failed_jobs_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = test_helpers.reset_db(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(_) =
    sqlight.exec(
      "INSERT INTO jobs_failed (
      id, 
      queue, 
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
      'default',                                   
      'process_order',                             
      '\"test-payload\"',       
      3,                                           
      'Test exception',
      '2024-09-29 10:30:00',                       
      '2024-09-29 10:30:00',
      '2024-09-29 11:00:00'                        
    );
    ",
      conn,
    )

  job_store.get_failed_jobs(1)
  |> pprint.format
  |> birdie.snap(title: "Get failed job from db")
}

pub fn increment_attempts_test() {
  use conn <- sqlight.with_connection("dispatch")
  let assert Ok(_) = test_helpers.reset_db(conn)
  let job_store = sqlite_store.try_new_store(conn)

  let assert Ok(job) = job_store.enqueue_job(queue_name, job_name, job_payload)

  job_store.increment_attempts(job)
  |> should.be_ok
  |> fn(j) { j.attempts }
  |> should.equal(1)
}

// Helpers
fn validate_job(
  job: job.Job,
  queue_name: String,
  job_name: String,
  job_payload: String,
) {
  job.queue
  |> should.equal(queue_name)

  job.name
  |> should.equal(job_name)

  job.payload
  |> should.equal(job_payload)

  should.equal(job.created_at, job.available_at)

  birl.compare(birl.now(), birl.from_erlang_universal_datetime(job.created_at))
  |> should.equal(order.Gt)

  birl.compare(
    birl.now(),
    birl.from_erlang_universal_datetime(job.available_at),
  )
  |> should.equal(order.Gt)
}
