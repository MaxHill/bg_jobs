import bg_jobs/queue
import chip
import gleam/erlang/process

type Registry(payload) =
  chip.Registry(queue.Message(payload), String, Nil)

pub type Context(payload) {
  Context(
    caller: process.Subject(Registry(payload)),
    registry: Registry,
    id: Int,
  )
}

pub fn setup() {
  let parent = process.new_subject()
  let queue_store = chip.new(parent)
}
