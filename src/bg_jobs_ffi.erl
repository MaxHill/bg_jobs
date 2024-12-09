-module(bg_jobs_ffi).
-export([pid_to_list/1, list_to_pid/1, pid_to_binary/1, binary_to_pid/1]).

%% Converts a pid to its list representation (character list)
pid_to_list(Pid) when is_pid(Pid) ->
    erlang:pid_to_list(Pid).

%% Converts a list representation (character list) back to a pid
list_to_pid(PidList) when is_list(PidList) ->
    erlang:list_to_pid(PidList).

%% Converts a pid to a binary
pid_to_binary(Pid) when is_pid(Pid) ->
    erlang:list_to_binary(erlang:pid_to_list(Pid)).

%% Converts a binary back to a pid
binary_to_pid(PidBinary) when is_binary(PidBinary) ->
    erlang:list_to_pid(binary_to_list(PidBinary)).


