-module(base_de_datos).
-include("replica.erl").
-export[start/2,stop/0]

start(Name,CantReplicas) ->
    Names=generate_replicas_names(Name,CantReplicas,[]),
    start_replicas(Names,Names),
    PID = spawn(base_de_datos,loop,[Name,CantReplicas]),
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

start_replicas([]) ->
    ok;
start_replicas([CurrName | LeftNames], Names) ->
    [CurrName, ListNames] = generate_correct_arguments(CurrName, Names),
    replica:start(CurrName, ListNames),
    start_replicas(LeftNames, Names).

generate_correct_arguments(Name, ListNames) ->
    NewListNames = lists:subtract(ListNames, [Name]),
    [Name, ListNames].

generate_replicas_names(Name, 0, Names) ->
    Names;
generate_replicas_names(Name, CantReplicas, Names) when CantReplicas > 0 ->
    generate_replicas_names(Name, CantReplicas - 1, [Name ++ "_" ++ CantReplicas | Names]).
