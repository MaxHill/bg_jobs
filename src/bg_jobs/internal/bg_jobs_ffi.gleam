import gleam/erlang/process

@external(erlang, "bg_jobs_ffi", "pid_to_binary")
pub fn pid_to_binary_ffi(pid: process.Pid) -> String

@external(erlang, "bg_jobs_ffi", "binary_to_pid")
pub fn binary_to_pid_ffi(str: String) -> process.Pid
