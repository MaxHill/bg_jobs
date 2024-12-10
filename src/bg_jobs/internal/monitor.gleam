import bg_jobs/db_adapter
import bg_jobs/events
import bg_jobs/internal/queue_messages
import bg_jobs/internal/scheduled_jobs_messages
import bg_jobs/internal/utils
import gleam/erlang/process
import gleam/function
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import lamb
import lamb/query

pub const name = "monitor"

pub const table_name = "monitor_ets_table"

pub fn register_queue(
  subject: process.Subject(queue_messages.Message),
  name: String,
) {
  use monitor_subject <- option.map(get_monitor_subject())
  actor.send(monitor_subject, RegisterQueue(subject, name))
}

pub fn register_scheduled_job(
  subject: process.Subject(scheduled_jobs_messages.Message),
  name: String,
) {
  use monitor_subject <- option.map(get_monitor_subject())
  actor.send(monitor_subject, RegisterScheduledJob(subject, name))
}

pub type State {
  State(
    self: process.Subject(Message),
    monitoring: MonitorTable,
    db_adapter: db_adapter.DbAdapter,
    send_event: fn(events.Event) -> Nil,
  )
}

pub fn build(
  monitor_table table: lamb.Table(String, MonitorTableValue),
  db_adapter db_adapter: db_adapter.DbAdapter,
) -> Result(process.Subject(Message), actor.StartError) {
  actor.start_spec(actor.Spec(
    init: fn() {
      let self = process.new_subject()

      lamb.insert(
        table,
        name,
        MonitorMonitor(process.subject_owner(self), "MonitorActor", self),
      )

      let state =
        State(
          db_adapter: db_adapter,
          self:,
          monitoring: table,
          send_event: events.send_event([], _),
        )

      // Release all
      process.send(self, Init)

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

// Monitor
//---------------
pub type Message {
  Shutdown
  Init
  /// Register a new actor process for monitoring.
  ///
  /// Allows monitor to start tracking the actor,
  /// enabling lifecycle management and job reservation tracking.
  RegisterQueue(process.Subject(queue_messages.Message), name: String)
  RegisterScheduledJob(
    process.Subject(scheduled_jobs_messages.Message),
    name: String,
  )
  /// Handle the termination of a monitored actor process.
  ///
  /// Triggers cleanup operations for jobs associated with the 
  /// terminated process.
  Down(process.ProcessDown)

  /// Proactively releases job reservations for processes that are no longer alive.
  ///
  /// This message serves as a fallback mechanism for job reservation cleanup. While 
  /// job reservations are typically released automatically when a process dies, this 
  /// message provides an additional safety net to handle scenarios where the standard 
  /// down message might have been missed or not processed.
  ///
  ReleaseAbandonedReservations
}

fn loop(message: Message, state: State) -> actor.Next(Message, State) {
  case message {
    Shutdown -> actor.Stop(process.Normal)
    RegisterQueue(subject, name) -> {
      let pid = process.subject_owner(subject)
      let selector =
        register(
          state,
          MonitorQueue(
            pid:,
            name:,
            process_monitor: process.monitor_process(pid),
            subject:,
          ),
        )

      actor.Continue(state, option.Some(selector))
    }
    RegisterScheduledJob(subject, name) -> {
      let pid = process.subject_owner(subject)
      let selector =
        register(
          state,
          MonitorScheduledJob(
            pid: pid,
            name:,
            process_monitor: process.monitor_process(pid),
            subject:,
          ),
        )
      actor.Continue(state, option.Some(selector))
    }
    Down(d) -> {
      state.send_event(events.MonitorReleasingReserved(d.pid))

      // Remove from monitoring list
      remove_from_monitor_table(state.monitoring, d.pid)

      // Release claimed jobs in database
      let assert Ok(released) =
        state.db_adapter.release_jobs_reserved_by(utils.pid_to_string(d.pid))
      released
      |> list.each(fn(job) { state.send_event(events.MonitorReleasedJob(job)) })

      // Update selector 
      let selector = update_subject(state)

      actor.Continue(state, option.Some(selector))
    }
    Init -> {
      actor.send(state.self, ReleaseAbandonedReservations)
      let selector =
        get_all_monitoring(state.monitoring)
        |> list.map(fn(m) { m.1 })
        |> list.filter(fn(m) {
          // Demonitor process. 
          //(It will be monitored again further down in the register call)
          case m {
            MonitorMonitor(_, _, _) -> Nil
            MonitorQueue(_, _, _, process_monitor)
            | MonitorScheduledJob(_, _, _, process_monitor) -> {
              process.demonitor_process(process_monitor)
            }
          }

          // Remove dead processes from monitor table (and this list)
          case process.is_alive(m.pid) {
            False -> {
              remove_from_monitor_table(state.monitoring, m.pid)
              False
            }
            True -> True
          }
        })
        // Select all monitored and self, only select self if list is empty
        |> list.fold(
          process.new_selector()
            |> process.selecting(state.self, function.identity),
          fn(acc, m) {
            case m {
              MonitorMonitor(_, _, _) -> acc
              MonitorQueue(_, _, _, _) | MonitorScheduledJob(_, _, _, _) -> {
                register(state, m)
              }
            }
          },
        )

      actor.Continue(state, option.Some(selector))
    }
    ReleaseAbandonedReservations -> {
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
            remove_from_monitor_table(state.monitoring, pid)
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
        MonitorQueue(_, _, _, m) | MonitorScheduledJob(_, _, _, m) -> {
          process.selecting_process_down(sel, m, Down)
        }
      }
    })
  }
}

fn register(state: State, value: MonitorTableValue) {
  add_to_monitor_table(state.monitoring, value.pid, value)
  update_subject(state)
}

// Monitor table
//---------------

@internal
pub fn get_monitor_subject() {
  use table <- option.then(get_table() |> option.from_result)

  lamb.search(table, query.new() |> query.index(name))
  |> list.first()
  |> option.from_result()
  |> option.then(fn(r: #(process.Pid, MonitorTableValue)) {
    case r.1 {
      MonitorMonitor(_, _, subject) -> option.Some(subject)
      _ -> option.None
    }
  })
}

@internal
pub fn add_to_monitor_table(
  table: MonitorTable,
  pid: process.Pid,
  monitor: MonitorTableValue,
) {
  lamb.insert(table, utils.pid_to_string(pid), monitor)
}

@internal
pub fn remove_from_monitor_table(table: MonitorTable, pid: process.Pid) -> Int {
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

@internal
pub fn get_by_name(
  table: MonitorTable,
  name: String,
) -> option.Option(MonitorTableValue) {
  lamb.search(table, query.new())
  |> list.reverse()
  |> list.find(fn(r: #(_, MonitorTableValue)) { { r.1 }.name == name })
  |> result.map(fn(r) { r.1 })
  |> option.from_result()
}

pub type MonitorTable =
  lamb.Table(String, MonitorTableValue)

pub type MonitorTableValue {
  MonitorQueue(
    pid: process.Pid,
    name: String,
    subject: process.Subject(queue_messages.Message),
    process_monitor: process.ProcessMonitor,
  )
  MonitorScheduledJob(
    pid: process.Pid,
    name: String,
    subject: process.Subject(scheduled_jobs_messages.Message),
    process_monitor: process.ProcessMonitor,
  )
  MonitorMonitor(
    pid: process.Pid,
    name: String,
    subject: process.Subject(Message),
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
