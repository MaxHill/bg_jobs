import bg_jobs
import sqlight

pub fn reset_db(connection: sqlight.Connection) {
  let assert Ok(_) = bg_jobs.migrate_down(connection)
  let assert Ok(_) = bg_jobs.migrate_up(connection)
}
