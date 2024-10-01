import bg_jobs/sqlite_store
import sqlight

pub fn reset_db(connection: sqlight.Connection) {
  let assert Ok(_) = sqlite_store.migrate_down(connection)()
  let assert Ok(_) = sqlite_store.migrate_up(connection)()
}
