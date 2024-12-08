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
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import lamb
import lamb/query

pub const name = "monitor"

pub const table_name = "monitor_ets_table"

pub fn register(
  monitor: process.Subject(messages.Message),
  actor: process.Subject(_),
) {
  let pid = process.subject_owner(actor)
  actor.send(monitor, messages.Register(pid))
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

      chip.register(
        registry,
        chip.new(self)
          |> chip.tag(name),
      )

      let state =
        State(
          db_adapter: db_adapter,
          self:,
          monitoring: initialize_named_registries_store(table_name),
          send_event: events.send_event([], _),
        )

      // Release all
      process.send(self, messages.ReleaseAbandonedReservations)

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
    messages.Register(pid) -> {
      add_monitor(state.monitoring, pid, process.monitor_process(pid))

      // Update selector 
      let selector = update_subject(state)

      actor.Continue(state, option.Some(selector))
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
      process.selecting_process_down(sel, mon.1, messages.Down)
    })
  }
}

// Ets store
//---------------

@internal
pub fn add_monitor(
  table: MonitorTable,
  pid: process.Pid,
  monitor: process.ProcessMonitor,
) -> Nil {
  lamb.insert(table, pid, monitor)
}

@internal
pub fn remove_monitor(table: MonitorTable, pid: process.Pid) -> Int {
  lamb.remove(table, query.new() |> query.index(pid))
}

@internal
pub fn get_all_monitoring(
  table: MonitorTable,
) -> List(#(process.Pid, process.ProcessMonitor)) {
  lamb.search(table, query.new())
}

@internal
pub fn get_by_pid(
  table: MonitorTable,
  pid: process.Pid,
) -> List(#(process.Pid, process.ProcessMonitor)) {
  lamb.search(table, query.new() |> query.index(pid))
}

pub type MonitorTable =
  lamb.Table(process.Pid, process.ProcessMonitor)

pub type MonitorTableValue {
  MonitorQueue(
    pid: process.Pid,
    process_monitor: process.ProcessMonitor,
    subject: process.Subject(queue_messages.Message),
  )
  MonitorScheduledJob(
    pid: process.Pid,
    process_monitor: process.ProcessMonitor,
    subject: process.Subject(scheduled_jobs_messages.Message),
  )
  MonitorMonitor(
    pid: process.Pid,
    process_monitor: process.ProcessMonitor,
    subject: process.Subject(messages.Message),
  )
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
