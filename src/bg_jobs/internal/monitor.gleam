import bg_jobs/db_adapter
import bg_jobs/events
import bg_jobs/internal/monitor_messages as messages
import bg_jobs/internal/registries
import bg_jobs/internal/utils
import chip
import gleam/erlang/process
import gleam/function
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor

pub const name = "monitor"

pub fn register(
  monitor: process.Subject(messages.Message),
  actor: process.Subject(_),
) {
  let pid = process.subject_owner(actor)
  actor.send(monitor, messages.Register(pid))
}

pub type Monitoring {
  Monitoring(pid: process.Pid, monitor: process.ProcessMonitor)
}

pub type State {
  State(
    self: process.Subject(messages.Message),
    monitoring: List(Monitoring),
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
        State(db_adapter:, self:, monitoring: [], send_event: events.send_event(
          [],
          _,
        ))

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
      let entry = Monitoring(pid, process.monitor_process(pid))
      let monitoring = list.append(state.monitoring, [entry])

      let selector =
        process.new_selector()
        |> process.selecting(state.self, function.identity)
        |> fn(sel) {
          monitoring
          |> list.fold(sel, fn(sel, mon) {
            process.selecting_process_down(sel, mon.monitor, messages.Down)
          })
        }

      actor.Continue(
        State(..state, monitoring: monitoring),
        option.Some(selector),
      )
    }
    messages.Down(d) -> {
      state.send_event(events.MonitorReleasingReserved(d.pid))

      // Remove from monitoring list
      let monitoring = list.filter(state.monitoring, fn(m) { m.pid != d.pid })

      // Update selector 
      let selector =
        process.new_selector()
        |> process.selecting(state.self, function.identity)
        |> fn(sel) {
          monitoring
          |> list.fold(sel, fn(sel, mon) {
            process.selecting_process_down(sel, mon.monitor, messages.Down)
          })
        }

      // Release claimed jobs in database
      let assert Ok(released) =
        state.db_adapter.release_jobs_reserved_by(utils.pid_to_string(d.pid))
      released
      |> list.each(fn(job) { state.send_event(events.MonitorReleasedJob(job)) })

      actor.Continue(
        State(..state, monitoring: monitoring),
        option.Some(selector),
      )
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
