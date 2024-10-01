import gleam/io
import gleam/result
import sqlight

pub fn main() {
  io.println("Hello from bg_jobs!")
}

pub fn migrate_up(conn: sqlight.Connection) {
  let sql =
    "
    CREATE TABLE IF NOT EXISTS jobs (
      id VARCHAR PRIMARY KEY NOT NULL,
      queue VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL,
      available_at DATETIME DEFAULT CURRENT_TIMESTAMP  NOT NULL,
      reserved_at DATETIME
    );
    "

  let sql_failed =
    "
    CREATE TABLE IF NOT EXISTS jobs_failed (
      id VARCHAR PRIMARY KEY NOT NULL,
      queue VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      exception VARCHAR NOT NULL,
      created_at DATETIME NOT NULL,
      available_at DATETIME NOT NULL,
      failed_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
    "

  let sql_succeded =
    "
    CREATE TABLE IF NOT EXISTS jobs_succeeded (
      id VARCHAR PRIMARY KEY NOT NULL,
      queue VARCHAR NOT NULL,
      name VARCHAR NOT NULL,
      payload TEXT NOT NULL, 
      attempts INTEGER NOT NULL,
      created_at DATETIME NOT NULL,
      available_at DATETIME NOT NULL,
      succeded_at DATETIME DEFAULT CURRENT_TIMESTAMP NOT NULL
    );
    "

  use _ <- result.try(sqlight.exec(sql, conn))
  use _ <- result.try(sqlight.exec(sql_failed, conn))
  sqlight.exec(sql_succeded, conn)
}

pub fn migrate_down(conn: sqlight.Connection) {
  let sql = "DROP TABLE IF EXISTS jobs"
  let sql_failed = "DROP TABLE IF EXISTS jobs_failed"
  let sql_succeded = "DROP TABLE IF EXISTS jobs_succeeded"

  use _ <- result.try(sqlight.exec(sql, conn))
  use _ <- result.try(sqlight.exec(sql_failed, conn))
  sqlight.exec(sql_succeded, conn)
}
