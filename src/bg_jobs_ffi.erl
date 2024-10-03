-module(bg_jobs_ffi).

-export([queue_type_name_to_string/1, function_name_to_string/1]).

queue_type_name_to_string(Varfn) ->
  {Name, {_, _}} = Varfn({nil, nil}),
  atom_to_binary(Name).



% The function takes a function as an argument
function_name_to_string(Fun) ->
    % Use fun_info to get the metadata about the function
    FunInfo = erlang:fun_info(Fun),
    
    % Extract the 'name' field from the function info (which will be an atom)
    {module, Module} = lists:keyfind(module, 1, FunInfo),
    
    % Convert the atom name to a string (binary)
    atom_to_binary(Module).
