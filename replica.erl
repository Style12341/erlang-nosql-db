-module(replica).
-behaviour(gen_server).
-define(DELETE_VALUE, n2FlOTg0OWYtY2E4Zi00NjBhLTljNjgtYjQzNzQ1ZjYyZjAw).
-define(FAKE_PID, make_ref()).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/2, stop/1, put/5, del/4, get/3]).

start(Name, ListReplicas) ->
    %Estado Inicial {Datos,Lista de replicas, Datos de pedidos}
    %Datos de pedidos es un diccionario con un numero de PID de cliente como clave y una lista de tuplas {Operacion,ExpectedResponses,Responses,BestValue} como valor
    gen_server:start_link({local, Name}, ?MODULE, {dict:new(), ListReplicas, dict:new()}, []).

init(Args) ->
    {ok, Args}.
terminate(_Reason, _Data) ->
    ok.
stop(Name) ->
    gen_server:cast(Name, stop).

get(Key, Consistency, Name) ->
    gen_server:call(Name, {get, Key, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.
del(Key, Ts, Consistency, Name) ->
    gen_server:call(Name, {del, Key, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.
put(Key, Value, Ts, Consistency, Name) ->
    gen_server:call(Name, {put, Key, Value, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.
replica_get(PidCoordinador, Ref, Key, Replica) ->
    io:format("[replica_get] PidCoordinador=~p, Ref=~p, Key=~p, Replica=~p~n", [
        PidCoordinador, Ref, Key, Replica
    ]),
    gen_server:call(Replica, {replica_get, PidCoordinador, Ref, Key}).
replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica) ->
    io:format("[replica_put] PidCoordinador=~p, Ref=~p, Key=~p, Value=~p, Ts=~p, Replica=~p~n", [
        PidCoordinador, Ref, Key, Value, Ts, Replica
    ]),
    gen_server:call(Replica, {replica_put, Key, Value, Ts, PidCoordinador, Ref}).
replica_del(PidCoordinador, Ref, Key, Ts, Replica) ->
    io:format("[replica_del] PidCoordinador=~p, Ref=~p, Key=~p, Ts=~p, Replica=~p~n", [
        PidCoordinador, Ref, Key, Ts, Replica
    ]),
    gen_server:call(Replica, {replica_del, Key, Ts, PidCoordinador, Ref}).

handle_call({put, Key, Value, Ts, Cons}, From, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] put: Key=~p, Value=~p, Ts=~p, Cons=~p~n", [Key, Value, Ts, Cons]),
    {Pid,_} = From,
    NewOrderData = generate_order(Pid, ListReplicas, Cons, OrderData, put),
    NewShinyData = new_order(Pid, {Key, Value, Ts}, Data, Cons, ListReplicas, put),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}};
handle_call({replica_put, Key, Value, Ts, PidCoordinador, Ref}, _, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] replica_put: Key=~p, Value=~p, Ts=~p~n", [Key, Value, Ts]),
    {BestValue, NewData} = put_value(Key, Value, Ts, Data),
    PidCoordinador ! {fulfill_order, Ref, BestValue},
    {reply, ok, {NewData, ListReplicas, OrderData}};
handle_call({del, Key, Ts, Cons}, From, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] del: Key=~p, Ts=~p, Cons=~p~n", [Key, Ts, Cons]),
    {Pid,_} = From,
    NewOrderData = generate_order(Pid, ListReplicas, Cons, OrderData, del),
    NewShinyData = new_order(Pid, {Key, Ts}, Data, Cons, ListReplicas, del),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}};
handle_call({replica_del, Key, Ts, PidCoordinador, Ref}, _, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] replica_del: Key=~p, Ts=~p~n", [Key, Ts]),
    {BestValue, NewData} = delete_value(Key, Ts, Data),
    PidCoordinador ! {fulfill_order, Ref, BestValue},
    {reply, ok, {NewData, ListReplicas, OrderData}};
handle_call({get, Key, Cons}, From, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] get: Key=~p, Cons=~p~n", [Key, Cons]),
    % Pid -> {get, ExpectedResponses, Responses, BestValue}
    {Pid,_} = From,
    NewOrderData = generate_order(Pid, ListReplicas, Cons, OrderData, get),
    NewShinyData = new_order(Pid, {Key}, Data, Cons, ListReplicas, get),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}};
handle_call({replica_get, PidCoordinador, Ref, Key}, _, {Data, ListReplicas, OrderData}) ->
    io:format("[handle_call] replica_get: Key=~p CoordinatorPid=~p~n", [Key,PidCoordinador]),
    Value = get_value(Key, Data),
    PidCoordinador ! {fulfill_order, Ref, Value},
    {reply, ok, {Data, ListReplicas, OrderData}}.
handle_cast(stop, _State) ->
    {stop, normal, ok}.
handle_info({fulfill_order, Ref, Value} =Info, {Data, ReplicaList, OrderData} =State) ->
    %Print the value
    io:format("[handle_info] fulfillorder: info: ~p state: ~p pidReceiver: ~p~n", [Info,State,self()]),
    {Op, ExpectedResponses, Responses, BestValue} = dict:fetch(Ref, OrderData),
    io:format("[handle_info] fulfillorder: Op=~p, ExpectedResponses=~p, Responses=~p, BestValue=~p~n", [
        Op, ExpectedResponses, Responses, BestValue
    ]),
    NewBestValue = compare_values(Value, BestValue),
    % PidNode ! {reply, NewBestValue},
    NewResponses = Responses + 1,
    case NewResponses of
        ExpectedResponses ->
            io:format("[handle_info] Answering Value: ~p to PID ~p~n", [ExpectedResponses,Ref]),
            Ref ! {reply, NewBestValue},
            NewOrderData = dict:erase(Ref, OrderData);
        _ ->
            NewOrderData = dict:store(
                Ref, {Op, ExpectedResponses, NewResponses, NewBestValue}, OrderData
            )
    end,
    {noreply, {Data, ReplicaList, NewOrderData}};
handle_info(_Info, State) ->
    io:format("[handle_info] unknown info: ~p state: ~p pidReceiver: ~p~n", [_Info,State,self()]),
    {noreply, State}.
generate_order(Ref, ListReplicas, Consistency, OrderData, Op) ->
    io:format("[generate_order] Ref=~p, Consistency=~p, Op=~p~n", [Ref, Consistency, Op]),
    dict:store(
        Ref, {Op, get_expected_responses(length(ListReplicas), Consistency), 0, {not_found}}, OrderData
    ).
get_expected_responses(Length, Consistency) ->
    io:format("[get_expected_responses] Lenght=~p, Consistency=~p~n", [Length, Consistency]),
    case Consistency of
        one -> 1;
        quorum -> (Length) div 2 + 1;
        all -> Length
    end.

new_order(Ref, OpData, Data, one, _, Op) ->
    io:format("[new_order] one: Ref=~p, OpData=~p, Op=~p~n", [Ref, OpData, Op]),
    {BestValue, NewData} = apply_operation(Op, OpData, Data),
    PidCoordinador = self(),
    PidCoordinador ! {fulfill_order, Ref, BestValue},
    NewData;
new_order(Ref, OpData, Data, quorum, ListReplicas, Op) ->
    io:format("[new_order] quorum: Ref=~p, OpData=~p, Op=~p~n", [Ref, OpData, Op]),
    {BestValue, NewData} = apply_operation(Op, OpData, Data),
    PidCoordinador = self(),
    PidCoordinador ! {fulfill_order, Ref, BestValue},
    Size = length(ListReplicas) div 2,
    NewList = lists:sublist(ListReplicas, Size),
    io:format("[new_order] quorum: Size=~p, NewList=~p~n", [Size, NewList]),
    PropagateOpList = lists:subtract(ListReplicas, NewList),
    request_order_fullfilment(Ref, PidCoordinador, OpData, NewList, Op),
    propagate_operation(Op, OpData, PropagateOpList),
    NewData;
new_order(Ref, OpData, Data, all, ListReplicas, Op) ->
    io:format("[new_order] all: Ref=~p, OpData=~p, Op=~p~n", [Ref, OpData, Op]),
    {BestValue, NewData} = apply_operation(Op, OpData, Data),
    PidCoordinador = self(),
    PidCoordinador ! {fulfill_order, Ref, BestValue},
    request_order_fullfilment(Ref, PidCoordinador, OpData, ListReplicas, Op),
    NewData.
request_order_fullfilment(_, _, _, [], _) ->
    io:format("[request_order_fullfilment] done~n"),
    ok;
request_order_fullfilment(Ref, PidCoordinador, OpData, [Replica | Rest], Op) ->
    io:format("[request_order_fullfilment] Ref=~p, Replica=~p, Op=~p~n", [Ref, Replica, Op]),
    apply_replica_operation(Op, OpData, PidCoordinador, Ref, Replica),
    request_order_fullfilment(Ref, PidCoordinador, OpData, Rest, Op).
propagate_operation(_, _, []) ->
    io:format("[propagate_operation] done~n"),
    ok;
propagate_operation(Op, OpData, [Replica | Rest]) ->
    io:format("[propagate_operation] Op=~p, Replica=~p~n", [Op, Replica]),
    % Generate a fake pid for the coordinator
    apply_replica_operation(Op, OpData, ?FAKE_PID, ?FAKE_PID, Replica),
    propagate_operation(Op, OpData, Rest).

compare_values({ok, _, Ts} = BestValue, {ko, BestTs}) when Ts > BestTs -> BestValue;
compare_values({ok, _, _}, {ko, _} = BestValue) -> BestValue;
compare_values({ok, _, Ts} = BestValue, {ok, _, BestTs}) when Ts > BestTs -> BestValue;
compare_values({ok, _, _}, {ok, _, _} = BestValue) -> BestValue;
compare_values({ko, _} = Arg2, {ok, _, _} = Arg1) -> compare_values(Arg1, Arg2);
compare_values({ko, Ts}, {ko, BestTs}) when Ts < BestTs -> {ko, BestTs};
compare_values({ko, BestTs}, {ko, _}) -> {ko, BestTs};
compare_values({not_found}, BestValue) -> BestValue;
compare_values(BestValue, {not_found}) -> BestValue.

apply_operation(get, {Key}, Data) ->
    io:format("[apply_operation] get: Key=~p~n", [Key]),
    BestValue = get_value(Key, Data),
    {BestValue, Data};
apply_operation(put, {Key, Value, Ts}, Data) ->
    io:format("[apply_operation] put: Key=~p, Value=~p, Ts=~p~n", [Key, Value, Ts]),
    put_value(Key, Value, Ts, Data);
apply_operation(del, {Key, Ts}, Data) ->
    io:format("[apply_operation] del: Key=~p, Ts=~p~n", [Key, Ts]),
    delete_value(Key, Ts, Data).
apply_replica_operation(get, {Key}, PidCoordinador, Ref, Replica) ->
    io:format("[apply_replica_operation] get: Key=~p, Replica=~p~n", [Key, Replica]),
    replica_get(PidCoordinador, Ref, Key, Replica);
apply_replica_operation(put, {Key, Value, Ts}, PidCoordinador, Ref, Replica) ->
    io:format("[apply_replica_operation] put: Key=~p, Value=~p, Ts=~p, Replica=~p~n", [
        Key, Value, Ts, Replica
    ]),
    replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica);
apply_replica_operation(del, {Key, Ts}, PidCoordinador, Ref, Replica) ->
    io:format("[apply_replica_operation] del: Key=~p, Ts=~p, Replica=~p~n", [Key, Ts, Replica]),
    replica_del(PidCoordinador, Ref, Key, Ts, Replica).
get_value(Key, Data) ->
    io:format("[get_value] Key=~p~n", [Key]),
    case dict:find(Key, Data) of
        {ok, {?DELETE_VALUE, Ts}} -> {ko, Ts};
        {ok, {Val, Ts}} -> {ok, Val, Ts};
        _ -> {not_found}
    end.
delete_value(Key, Ts, Data) ->
    io:format("[delete_value] Key=~p, Ts=~p~n", [Key, Ts]),
    case get_value(Key, Data) of
        {ko, OldTs} when OldTs > Ts -> {{not_found}, Data};
        {ko, _} -> {{not_found}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {ok, _, OldTs} when OldTs > Ts -> {{ko, OldTs}, Data};
        {ok, _, _} -> {{ok, Ts, Ts}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {not_found} -> {{not_found}, Data}
    end.
put_value(Key, Value, Ts, Data) ->
    io:format("[put_value] Key=~p, Value=~p, Ts=~p~n", [Key, Value, Ts]),
    case get_value(Key, Data) of
        {ok, _, OldTs} when OldTs > Ts -> {{ko, OldTs}, Data};
        {ko, OldTs} when OldTs > Ts -> {{not_found}, Data};
        _ -> {{ok, Value, Ts}, dict:store(Key, {Value, Ts}, Data)}
    end.
