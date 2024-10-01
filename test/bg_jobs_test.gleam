import bg_jobs
import birdie
import gleam/dynamic
import gleeunit
import pprint
import sqlight

pub fn main() {
  gleeunit.main()
}

pub fn migrate_test() {
  use conn <- sqlight.with_connection(":memory:")

  let assert Ok(_) = bg_jobs.migrate_up(conn)

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
  |> pprint.format
  |> birdie.snap(title: "table names after migrate up")

  let assert Ok(_) = bg_jobs.migrate_down(conn)
  sqlight.query(
    sql,
    conn,
    [],
    dynamic.decode1(fn(str) { str }, dynamic.element(0, dynamic.string)),
  )
  |> pprint.format
  |> birdie.snap(title: "table names after migrate down")
}
