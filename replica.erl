-module(replica).
-behaviour(gen_server).
-define(DELETE_VALUE, "7ae9849f-ca8f-460a-9c68-b43745f62f00").
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/2, stop/1, put/5, del/4, get/3]).

start(Name, ListReplicas) ->
    %Estado Inicial {Datos,Lista de replicas, Datos de pedidos}
    %Datos de pedidos es un diccionario con un numero de PID de cliente como clave y una lista de tuplas {Operacion,ExpectedResponses,Responses,BestValue} como valor
    gen_server:start_link({global, Name}, ?MODULE, {dict:new(), ListReplicas,Data}, []).

init(Args) ->
    {ok, Args}.
terminate(_Reason, _Data) ->
    ok.
stop(Name) ->
    gen_server:cast(Name, stop).
get(Key, Consistency, Name) ->
    {wait , ref} =gen_server:call(Name, {get, Key, Consistency}),
    receive
      {reply,ref,Value} -> Value
    end.
replica_get(Key, Name) ->
    % Default consistency level
    gen_server:call(Name, {get, Key, one}).
del(Key, Ts, Consistency, Name) ->
    gen_server:cast(Name, {del, Key, Ts, Consistency}).
del(Key, Ts, Name) ->
    % Default consistency level
    gen_server:cast(Name, {del, Key, Ts, one}).

put(Key, Value, Ts, Consistency, Name) ->
    gen_server:call(Name, {put, Key, Value, Ts, Consistency}).
put(Key, Value, Ts, Name) ->
    % Default consistency level
    gen_server:call(Name, {put, Key, Value, Ts, one}).

handle_call({get, Key, Ts, Cons}, _From, {Data, ListReplicas}) ->
    Reply = get_consistent_value(Key, Data, Cons, ListReplicas),
    {reply, Reply, {Data, ListReplicas}}.

handle_info({reply_node,RefNumber,BestValue}, Data) ->
    {noreply, Data}.

get_consistent_value(Key, Data, one, _) ->
    get_value(Key, Data);
get_consistent_value(Key, Data, quorum, ListReplicas) ->
    LocalValue = get_value(Key, Data),
    Size = length(ListReplicas) / 2 + 1,
    NewList = lists:sublist(ListReplicas, Size),
    AllValues = [LocalValue | get_value_from_replicas(Key, NewList, Data)],
    get_valid_value(AllValues,LocalValue);
get_consistent_value(Key, Data, all, ListReplicas) ->
    LocalValue = get_value(Key, Data),
    AllValues = [LocalValue, get_value_from_replicas(Key, ListReplicas, Data)],
    get_valid_value(AllValues,LocalValue).

get_value_from_replicas(_, [], Data) ->
    Data;
get_value_from_replicas(Key, [CurrName | LeftNames], Data) ->
    Value = replica:get(Key, CurrName),
    get_value_from_replicas(Key, LeftNames, [Value, Data]).

get_valid_value([],Value) -> Value;
get_valid_value([CurrentValue|RestValues],BestValue) ->
    NewBest = compare_values(CurrentValue,BestValue),
    get_valid_value(RestValues,NewBest).


get_value(Key, Data) ->
    case dict:find(Key, Data) of
        {ok, {?DELETE_VALUE, Ts}} -> {ko,Ts};
        {ok, {Val, Ts}} -> {ok, Val, Ts};
        _ -> {not_found}
    end.

compare_values({ok,_,Ts} = BestValue,{ko,BestTs}) when Ts>BestTs -> BestValue;
compare_values({ok,_,_},{ko,_} = BestValue)  -> BestValue;
compare_values({ok,_,Ts} = BestValue,{ok,_,BestTs}) when Ts>BestTs -> BestValue;
compare_values({ok,_,_},{ok,_,_} = BestValue)-> BestValue;
compare_values({ko,_} = Arg2,{ok,_,_} = Arg1) -> compare_values(Arg1,Arg2);
compare_values({not_found},BestValue) -> BestValue;
compare_values(BestValue,{not_found}) -> BestValue.

