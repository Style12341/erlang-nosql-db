-module(replica).
-behaviour(gen_server).
-define(DELETE_VALUE, n2FlOTg0OWYtY2E4Zi00NjBhLTljNjgtYjQzNzQ1ZjYyZjAw).
-define(FAKE_PID, make_ref()).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/2, stop/1, put/5, del/4, get/3]).
-type fulfill_order_type() :: {fulfill_order, pid(), reference(), tuple(), tuple()}.
-type consistency() :: one | quorum | all.
-type key() :: any().
-type value() :: any().
-type ts() :: integer().
-type replica() :: atom().

%% @doc
%% Starts the replica server with the given name and list of replicas.
%% @spec start(atom(), list()) -> {ok, pid()} | {error, any()}
start(Name, ListReplicas) ->
    % Estado Inicial {Datos, Lista de replicas, Datos de pedidos}
    % Datos de pedidos es un diccionario con un numero de PID de cliente como clave y una lista de tuplas {Operacion, ExpectedResponses, Responses, BestValue} como valor
    gen_server:start_link({local, Name}, ?MODULE, {dict:new(), ListReplicas, dict:new()}, []).

%% @doc
%% Initializes the server state.
%% @spec init(any()) -> {ok, any()}
init(Args) ->
    {ok, Args}.

%% @doc
%% Terminates the server.
%% @spec terminate(any(), any()) -> ok
terminate(_Reason, _Data) ->
    ok.

%% @doc
%% Stops the replica server with the given name.
%% @spec stop(atom()) -> ok
stop(Name) ->
    gen_server:cast(Name, stop).

%% @doc
%% Gets the value associated with the given key from the replica server.
%% @spec get(any(), atom(), atom()) -> any()
get(Key, Consistency, Name) ->
    gen_server:call(Name, {get, Key, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%% @doc
%% Deletes the value associated with the given key from the replica server.
%% @spec del(any(), any(), atom(), atom()) -> any()
del(Key, Ts, Consistency, Name) ->
    gen_server:call(Name, {del, Key, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%% @doc
%% Puts the given value associated with the given key into the replica server.
-spec put(key(), value(), ts(), consistency(), replica()) -> any().
put(Key, Value, Ts, Consistency, Name) ->
    gen_server:call(Name, {put, Key, Value, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%% @doc
%% Handles the get request from a replica.
%% @spec replica_get(pid(), reference(), any(), atom()) -> any()
replica_get(PidCoordinador, Ref, Key, Replica) ->
    gen_server:cast(Replica, {replica_get, PidCoordinador, Ref, Key}).

%% @doc
%% Handles the put request from a replica.
%% @spec replica_put(pid(), reference(), any(), any(), any(), atom()) -> any()
replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica) ->
    gen_server:cast(Replica, {replica_put, Key, Value, Ts, PidCoordinador, Ref}).

%% @doc
%% Handles the delete request from a replica.
%% @spec replica_del(pid(), reference(), any(), any(), atom()) -> any()
replica_del(PidCoordinador, Ref, Key, Ts, Replica) ->
    gen_server:cast(Replica, {replica_del, Key, Ts, PidCoordinador, Ref}).
replica_fix(Key, Value, Replica) ->
    gen_server:cast(Replica, {replica_fix, Key, Value}).

%% @doc
%% Handles the call messages for the gen_server.
%% @spec handle_call(tuple(), {pid(), any()}, {dict(), list(), dict()}) -> {reply, any(), {dict(), list(), dict()}}
handle_call({put, Key, Value, Ts, Cons}, From, {Data, ListReplicas, OrderData}) ->
    {Pid, _} = From,
    NewOrderData = generate_order(Key, Pid, ListReplicas, Cons, OrderData, put),
    NewShinyData = new_order(Pid, {Key, Value, Ts}, Data, Cons, ListReplicas, put),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}};
handle_call({del, Key, Ts, Cons}, From, {Data, ListReplicas, OrderData}) ->
    {Pid, _} = From,
    NewOrderData = generate_order(Key, Pid, ListReplicas, Cons, OrderData, del),
    NewShinyData = new_order(Pid, {Key, Ts}, Data, Cons, ListReplicas, del),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}};
handle_call({get, Key, Cons}, From, {Data, ListReplicas, OrderData}) ->
    % Pid -> {get, ExpectedResponses, Responses, BestValue}
    {Pid, _} = From,
    NewOrderData = generate_order(Key, Pid, ListReplicas, Cons, OrderData, get),
    NewShinyData = new_order(Pid, {Key}, Data, Cons, ListReplicas, get),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}}.

%% @doc
%% Handles the cast messages for the gen_server.
%% @spec handle_cast(atom(), any()) -> {stop, normal, ok}
handle_cast({replica_put, Key, Value, Ts, PidCoordinador, Ref}, {Data, ListReplicas, OrderData}) ->
    {BestValue, NewData} = put_value(Key, Value, Ts, Data),
    SavedValue = get_value(Key, NewData),
    PidCoordinador ! {fulfill_order, self(), Ref, BestValue, SavedValue},
    {noreply, {NewData, ListReplicas, OrderData}};
handle_cast({replica_del, Key, Ts, PidCoordinador, Ref}, {Data, ListReplicas, OrderData}) ->
    {BestValue, NewData} = delete_value(Key, Ts, Data),
    SavedValue = get_value(Key, NewData),
    PidCoordinador ! {fulfill_order, self(), Ref, BestValue, SavedValue},
    {noreply, {NewData, ListReplicas, OrderData}};
handle_cast({replica_get, PidCoordinador, Ref, Key}, {Data, ListReplicas, OrderData}) ->
    Value = get_value(Key, Data),
    PidCoordinador ! {fulfill_order, self(), Ref, Value, Value},
    {noreply, {Data, ListReplicas, OrderData}};
handle_cast({replica_fix, Key, {ok, Value, Ts}}, {Data, ListReplicas, OrderData}) ->
    {_, NewData} = put_value(Key, Value, Ts, Data),
    {noreply, {NewData, ListReplicas, OrderData}};
handle_cast({replica_fix, Key, {ko, Ts}}, {Data, ListReplicas, OrderData}) ->
    {_, NewData} = delete_value(Key, Ts, Data),
    {noreply, {NewData, ListReplicas, OrderData}};
handle_cast({replica_fix, _, {not_found}}, {Data, ListReplicas, OrderData}) ->
    {noreply, {Data, ListReplicas, OrderData}};
handle_cast(stop, _State) ->
    {stop, normal, ok}.

%% @doc
%% Handles the info messages for the gen_server.
-spec handle_info(fulfill_order_type(), {dict:dict(), list(), dict:dict()}) ->
    {noreply, {dict:dict(), list(), dict:dict()}}.
handle_info(
    {fulfill_order, SenderPid, Ref, Value, SavedValue},
    {Data, ReplicaList, OrderData}
) ->
    % Print the value
    case dict:find(Ref, OrderData) of
        {ok, {Op, ExpectedResponses, Responses, BestValue, Key}} ->
            NewBestValue = compare_values(Value, BestValue),
            case NewBestValue of
                Value -> NewData = Data;
                _ -> {_, NewData} = put_value(Key, SavedValue, Data)
            end,
            ensure_sender_consistency(SenderPid, Key, SavedValue, NewData),
            % PidNode ! {reply, NewBestValue},
            NewResponses = Responses + 1,
            case NewResponses of
                ExpectedResponses ->
                    Ref ! {reply, format_client_response(Op, NewBestValue)},
                    NewOrderData = dict:erase(Ref, OrderData);
                _ ->
                    NewOrderData = dict:store(
                        Ref, {Op, ExpectedResponses, NewResponses, NewBestValue, Key}, OrderData
                    )
            end,
            {noreply, {NewData, ReplicaList, NewOrderData}};
        _ ->
            {noreply, {Data, ReplicaList, OrderData}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

ensure_sender_consistency(SenderPid, Key, SavedValue, Data) ->
    CurrentValue = get_value(Key, Data),
    BestValue = compare_values(CurrentValue, SavedValue),
    case BestValue of
        CurrentValue ->
            case CurrentValue of
                SavedValue ->
                    SavedValue;
                _ ->
                    replica_fix(Key, CurrentValue, SenderPid),
                    CurrentValue
            end;
        _ ->
            SenderPid ! {reply, BestValue},
            BestValue
    end.

%% @doc
%% Generates a new order for the given reference.
%% @spec generate_order(reference(), list(), atom(), dict(), atom()) -> dict()
generate_order(Key, Ref, ListReplicas, Consistency, OrderData, Op) ->
    dict:store(
        Ref,
        {Op, get_expected_responses(length(ListReplicas), Consistency), 0, {not_found}, Key},
        OrderData
    ).

%% @doc
%% Gets the expected number of responses based on the consistency level.
%% @spec get_expected_responses(integer(), atom()) -> integer()
get_expected_responses(Length, Consistency) ->
    case Consistency of
        one -> 1;
        quorum -> (Length) div 2 + 1;
        all -> Length + 1
    end.

%% @doc
%% Creates a new order based on the consistency level.
%% @spec new_order(reference(), tuple(), dict(), atom(), list(), atom()) -> dict()
new_order(Ref, OpData, Data, one, ListReplicas, Op) ->
    {BestValue, NewData} = apply_operation(Op, OpData, Data),
    PidCoordinador = self(),
    SavedValue = get_value(element(1, OpData), NewData),
    PidCoordinador ! {fulfill_order, self(), Ref, BestValue, SavedValue},
    propagate_operation(Op, OpData, ListReplicas),
    NewData;
new_order(Ref, OpData, Data, _, ListReplicas, Op) ->
    {BestValue, NewData} = apply_operation(Op, OpData, Data),
    PidCoordinador = self(),
    SavedValue = get_value(element(1, OpData), NewData),
    PidCoordinador ! {fulfill_order, self(), Ref, BestValue, SavedValue},
    request_order_fullfilment(Ref, PidCoordinador, OpData, ListReplicas, Op),
    NewData.

%% @doc
%% Requests the fulfillment of an order.
%% @spec request_order_fullfilment(reference(), pid(), tuple(), list(), atom()) -> ok
request_order_fullfilment(_, _, _, [], _) ->
    ok;
request_order_fullfilment(Ref, PidCoordinador, OpData, [Replica | Rest], Op) ->
    apply_replica_operation(Op, OpData, PidCoordinador, Ref, Replica),
    request_order_fullfilment(Ref, PidCoordinador, OpData, Rest, Op).

%% @doc
%% Propagates the operation to the replicas.
%% @spec propagate_operation(atom(), tuple(), list()) -> ok
propagate_operation(_, _, []) ->
    ok;
propagate_operation(Op, OpData, [Replica | Rest]) ->
    % Generate a fake pid for the coordinator
    apply_replica_operation(Op, OpData, ?FAKE_PID, ?FAKE_PID, Replica),
    propagate_operation(Op, OpData, Rest).

%% @doc
%% Compares two values and returns the best one.
%% @spec compare_values(tuple(), tuple()) -> tuple()
compare_values({ok, _, BestTs} = BestValue, {ko, Ts}) when BestTs > Ts -> BestValue;
compare_values({ok, _, _}, {ko, _} = BestValue) -> BestValue;
compare_values({ok, _, BestTs} = BestValue, {ok, _, Ts}) when BestTs > Ts -> BestValue;
compare_values({ok, _, _}, {ok, _, _} = BestValue) -> BestValue;
compare_values({ko, _} = Arg2, {ok, _, _} = Arg1) -> compare_values(Arg1, Arg2);
compare_values({ko, BestTs}, {ko, Ts}) when BestTs > Ts -> {ko, BestTs};
compare_values({ko, _}, {ko, BestTs}) -> {ko, BestTs};
compare_values({not_found}, BestValue) -> BestValue;
compare_values(BestValue, {not_found}) -> BestValue.

%% @doc
%% Applies the given operation to the data.
%% @spec apply_operation(atom(), tuple(), dict()) -> {tuple(), dict()}
apply_operation(get, {Key}, Data) ->
    BestValue = get_value(Key, Data),
    {BestValue, Data};
apply_operation(put, {Key, Value, Ts}, Data) ->
    put_value(Key, Value, Ts, Data);
apply_operation(del, {Key, Ts}, Data) ->
    delete_value(Key, Ts, Data).

%% @doc
%% Applies the given operation to a replica.
%% @spec apply_replica_operation(atom(), tuple(), pid(), reference(), atom()) -> any()
apply_replica_operation(get, {Key}, PidCoordinador, Ref, Replica) ->
    replica_get(PidCoordinador, Ref, Key, Replica);
apply_replica_operation(put, {Key, Value, Ts}, PidCoordinador, Ref, Replica) ->
    replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica);
apply_replica_operation(del, {Key, Ts}, PidCoordinador, Ref, Replica) ->
    replica_del(PidCoordinador, Ref, Key, Ts, Replica).

%% @doc
%% Gets the value associated with the given key from the data.
%% @spec get_value(any(), dict()) -> tuple()
get_value(Key, Data) ->
    case dict:find(Key, Data) of
        {ok, {?DELETE_VALUE, Ts}} -> {ko, Ts};
        {ok, {Val, Ts}} -> {ok, Val, Ts};
        _ -> {not_found}
    end.

%% @doc
%% Deletes the value associated with the given key from the data.
%% @spec delete_value(any(), any(), dict()) -> {tuple(), dict()}
delete_value(Key, Ts, Data) ->
    case get_value(Key, Data) of
        {ko, NewTs} when NewTs > Ts -> {{not_found}, Data};
        {ko, _} -> {{not_found}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {ok, _, NewTs} when NewTs > Ts -> {{ko, NewTs}, Data};
        {ok, _, _} -> {{ok, Ts, Ts}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {not_found} -> {{not_found}, Data}
    end.
%% @doc
%% Puts the given value associated with the given key into the data.
%% @spec put_value(any(), any(), any(), dict()) -> {tuple(), dict()}
put_value(Key, Value, Ts, Data) ->
    case get_value(Key, Data) of
        {ok, _, NewTs} when NewTs > Ts -> {{ko, NewTs}, Data};
        {ko, NewTs} when NewTs > Ts -> {{not_found}, Data};
        _ -> {{ok, Value, Ts}, dict:store(Key, {Value, Ts}, Data)}
    end.
put_value(Key, {ok, Value, Ts}, Data) ->
    put_value(Key, Value, Ts, Data);
put_value(Key, {ko, Ts}, Data) ->
    delete_value(Key, Ts, Data);
put_value(_, {not_found}, Data) ->
    {not_found, Data}.

format_client_response(get, Value) ->
    Value;
format_client_response(_, {ok, _, _}) ->
    {ok};
format_client_response(_, {ko, _}) ->
    {ko};
format_client_response(_, RES) ->
    RES.
