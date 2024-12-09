import bg_jobs/db_adapter
import bg_jobs/events
import bg_jobs/internal/monitor_messages as messages
import bg_jobs/internal/queue_messages
import bg_jobs/internal/registries
import bg_jobs/internal/scheduled_jobs_messages
import bg_jobs/internal/utils
import chip
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option
import gleam/otp/actor
import lamb
import lamb/query

pub const name = "monitor"

pub const table_name = "monitor_ets_table"

pub fn register_queue(
  subject: process.Subject(queue_messages.Message),
  name: String,
) {
  use monitor_subject <- option.map(get_monitor_subject())
  actor.send(monitor_subject, messages.RegisterQueue(subject, name))
}

pub fn register_scheduled_job(
  subject: process.Subject(scheduled_jobs_messages.Message),
  name: String,
) {
  use monitor_subject <- option.map(get_monitor_subject())
  actor.send(monitor_subject, messages.RegisterScheduledJob(subject, name))
}

pub type State {
  State(
    self: process.Subject(messages.Message),
    monitoring: MonitorTable,
    db_adapter: db_adapter.DbAdapter,
    send_event: fn(events.Event) -> Nil,
  )
}

// TODO: Take options
pub fn build(
  registry registry: registries.MonitorRegistry,
  db_adapter db_adapter: db_adapter.DbAdapter,
) -> Result(process.Subject(messages.Message), actor.StartError) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()
      let table = initialize_named_registries_store(table_name)

      lamb.insert(
        table,
        name,
        MonitorMonitor(process.subject_owner(self), self, "MonitorActor"),
      )

      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(name),
      )

      let state =
        State(
          db_adapter: db_adapter,
          self:,
          monitoring: table,
          send_event: events.send_event([], _),
        )

      // Release all
      process.send(self, messages.Init)

      actor.Ready(
        state,
        process.new_selector()
          |> process.selecting(self, function.identity),
      )
    },
    init_timeout: 1000,
    loop: loop,
  ))
}

fn loop(
  message: messages.Message,
  state: State,
) -> actor.Next(messages.Message, State) {
  case message {
    messages.Shutdown -> actor.Stop(process.Normal)
    messages.RegisterQueue(subject, name) -> {
      let pid = process.subject_owner(subject)
      register(
        state,
        MonitorQueue(
          pid:,
          process_monitor: process.monitor_process(pid),
          subject:,
          name:,
        ),
      )
    }
    messages.RegisterScheduledJob(subject, name) -> {
      let pid = process.subject_owner(subject)
      register(
        state,
        MonitorScheduledJob(
          pid:,
          process_monitor: process.monitor_process(pid),
          subject:,
          name:,
        ),
      )
    }
    messages.Down(d) -> {
      state.send_event(events.MonitorReleasingReserved(d.pid))

      // Remove from monitoring list
      remove_monitor(state.monitoring, d.pid)

      // Release claimed jobs in database
      let assert Ok(released) =
        state.db_adapter.release_jobs_reserved_by(utils.pid_to_string(d.pid))
      released
      |> list.each(fn(job) { state.send_event(events.MonitorReleasedJob(job)) })

      // Update selector 
      let selector = update_subject(state)

      actor.Continue(state, option.Some(selector))
    }
    messages.Init -> {
      // Remove all dead processes from monitoring table
      get_all_monitoring(state.monitoring)
      |> list.map(fn(process) {
        let value = process.1
        case process.is_alive(value.pid) {
          True -> {
            // Re-register the subjects (overriding the old) to 
            // setup process_monitor again
            case value {
              MonitorMonitor(_, _, _) -> option.None
              MonitorQueue(pid, subject, process_monitor, name) -> {
                process.demonitor_process(process_monitor)
                remove_monitor(state.monitoring, pid)
                register_queue(subject, name)
              }
              MonitorScheduledJob(pid, subject, process_monitor, name) -> {
                process.demonitor_process(process_monitor)
                remove_monitor(state.monitoring, pid)
                register_scheduled_job(subject, name)
              }
            }
            Nil
          }
          False -> {
            // remove monitoring if process is dead
            remove_monitor(state.monitoring, { process.1 }.pid)
            Nil
          }
        }
      })

      // Release all reservations by dead processes in the database
      actor.send(state.self, messages.ReleaseAbandonedReservations)
      actor.continue(state)
    }
    messages.ReleaseAbandonedReservations -> {
      let assert Ok(jobs) = state.db_adapter.get_running_jobs()

      jobs
      |> list.each(fn(job) {
        // Should always be some because we only select from 
        // the database where reserved_by is not null
        let assert option.Some(reserved_by) = job.reserved_by
        let pid = utils.string_to_pid(reserved_by)
        case process.is_alive(pid) {
          False -> {
            let assert Ok(_) =
              state.db_adapter.release_jobs_reserved_by(reserved_by)
            remove_monitor(state.monitoring, pid)
            Nil
          }
          True -> {
            Nil
          }
        }
      })
      // Get all reserved jobs from db
      // Check if process is alive
      // if not release reservation
      actor.continue(state)
    }
  }
}

fn update_subject(state: State) {
  process.new_selector()
  |> process.selecting(state.self, function.identity)
  |> fn(sel) {
    get_all_monitoring(state.monitoring)
    |> list.fold(sel, fn(sel, mon) {
      case mon.1 {
        MonitorMonitor(_, _, _) -> sel
        MonitorQueue(_, _, m, _) | MonitorScheduledJob(_, _, m, _) -> {
          process.selecting_process_down(sel, m, messages.Down)
        }
      }
    })
  }
}

fn register(state: State, value: MonitorTableValue) {
  add_monitor(state.monitoring, value.pid, value)

  // Update selector 
  let selector = update_subject(state)

  actor.Continue(state, option.Some(selector))
}

// Ets store
//---------------

// @internal
// pub fn insert_monitor(
//   table: MonitorTable,
//   pid: process.Pid,
//   value: MonitorTableValue,
// ) -> Nil {
//   lamb.insert(table, pid, value)
// }

@internal
pub fn get_monitor_subject() {
  use table <- option.then(get_table() |> option.from_result)

  lamb.search(table, query.new() |> query.index(name))
  |> list.first()
  |> option.from_result()
  |> option.then(fn(r: #(process.Pid, MonitorTableValue)) {
    case r.1 {
      MonitorMonitor(_, subject, _) -> option.Some(subject)
      _ -> option.None
    }
  })
}

@internal
pub fn add_monitor(
  table: MonitorTable,
  pid: process.Pid,
  monitor: MonitorTableValue,
) {
  lamb.insert(table, utils.pid_to_string(pid), monitor)
}

@internal
pub fn remove_monitor(table: MonitorTable, pid: process.Pid) -> Int {
  let key = utils.pid_to_string(pid)
  lamb.remove(table, query.new() |> query.index(key))
}

@internal
pub fn get_all_monitoring(
  table: MonitorTable,
) -> List(#(process.Pid, MonitorTableValue)) {
  lamb.search(table, query.new())
}

@internal
pub fn get_by_pid(
  table: MonitorTable,
  pid: process.Pid,
) -> List(#(process.Pid, MonitorTableValue)) {
  lamb.search(table, query.new() |> query.index(pid))
}

pub type MonitorTable =
  lamb.Table(String, MonitorTableValue)

pub type MonitorTableValue {
  MonitorQueue(
    pid: process.Pid,
    subject: process.Subject(queue_messages.Message),
    process_monitor: process.ProcessMonitor,
    name: String,
  )
  MonitorScheduledJob(
    pid: process.Pid,
    subject: process.Subject(scheduled_jobs_messages.Message),
    process_monitor: process.ProcessMonitor,
    name: String,
  )
  MonitorMonitor(
    pid: process.Pid,
    subject: process.Subject(messages.Message),
    name: String,
  )
}

@internal
pub fn get_table() -> Result(MonitorTable, Nil) {
  lamb.from_name(table_name)
}

@internal
pub fn initialize_named_registries_store(name) -> MonitorTable {
  case lamb.from_name(name) {
    Ok(table) -> table
    Error(Nil) ->
      case lamb.create(name, lamb.Public, lamb.Set, True) {
        Ok(table) -> table
        Error(_error) ->
          panic as { "Unexpected error trying to initialize ETS store" }
      }
  }
}
