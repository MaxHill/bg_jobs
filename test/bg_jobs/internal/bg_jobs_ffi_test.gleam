import bg_jobs/internal/bg_jobs_ffi
import gleam/erlang/process
import gleeunit/should

pub fn pid_to_string_test() {
  let pid =
    process.new_subject()
    |> process.subject_owner()

  pid
  |> bg_jobs_ffi.pid_to_binary_ffi()
  |> bg_jobs_ffi.binary_to_pid_ffi()
  |> should.equal(pid)
}
