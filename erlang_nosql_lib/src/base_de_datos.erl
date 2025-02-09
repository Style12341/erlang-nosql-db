-module(base_de_datos).
-export[start/2,stop/0,loop/2].

start(Name,CantReplicas) ->
    Names=generate_replicas_names(Name,CantReplicas,[]),
    start_replicas(Names,Names),
    PID = spawn(?MODULE,loop,[Name,CantReplicas]),
    register(?MODULE,PID).

stop() ->
    ?MODULE ! stop.

loop(Name, CantReplicas) ->
    receive
        stop -> stop_replicas(Name, CantReplicas)
    end.

stop_replicas(Name, CantReplicas) ->
    Names = generate_replicas_names(Name, CantReplicas, []),
    stop_replicas(Names).

stop_replicas([]) ->
    ok;
stop_replicas([CurrName | LeftNames]) ->
    replica:stop(CurrName),
    stop_replicas(LeftNames).

start_replicas([],_) ->
    ok;
start_replicas([CurrName | LeftNames], Names) ->
    [CurrName, ListNames] = generate_correct_arguments(CurrName, Names),
    replica:start(CurrName, ListNames),
    start_replicas(LeftNames, Names).

generate_correct_arguments(Name, ListNames) ->
    NewListNames = lists:subtract(ListNames, [Name]),
    [Name, NewListNames].

generate_replicas_names(_, 0, Names) ->
    Names;
generate_replicas_names(Name, CantReplicas, Names) when CantReplicas > 0 ->
    NewName = list_to_atom(Name ++ "_" ++ integer_to_list(CantReplicas)),
    generate_replicas_names(Name, CantReplicas - 1, [NewName | Names]).
