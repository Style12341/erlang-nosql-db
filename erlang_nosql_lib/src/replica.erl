-module(replica).
-behaviour(gen_server).
-define(DELETE_VALUE, n2FlOTg0OWYtY2E4Zi00NjBhLTljNjgtYjQzNzQ1ZjYyZjAw).
-define(FAKE_PID, make_ref()).

%% Define a record for the order -- 5 fields as expected.
-record(order, {
    op :: operation(),
    expected_responses :: response_qty(),
    responses :: response_qty(),
    best_value :: operation_value(),
    key :: key()
}).

%%%-------------------------------------------------------------------
%%% Exported functions
%%%-------------------------------------------------------------------
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/2, stop/1, put/5, del/4, get/3]).

%%%-------------------------------------------------------------------
%%% Types
%%%-------------------------------------------------------------------
-type consistency() :: one | quorum | all.
-type put_reply() :: {ok} | {ko} | {not_found}.
-type get_reply() :: {ok, value(), timestamp()} | {ko, timestamp()} | {not_found}.
-type del_reply() :: {ok} | {ko} | {not_found}.
-type timestamp() :: integer().
-type replica() :: atom().
-type key() :: term().
-type value() :: term().
-type operation() :: get | put | del.
-type order_ref() :: pid() | reference().
-type pid_ref() :: pid() | reference().
-type dict_data() :: dict:dict(key(), {value(), timestamp()}).
-type operation_value() :: get_reply().
-type operation_data() :: {key()} | {key(), value(), timestamp()} | {key(), timestamp()}.
-type response_qty() :: integer().

%% Instead of tuple orders, we use our record.
-type dict_order() :: dict:dict(order_ref(), #order{}).

-type state() :: {dict_data(), list(replica()), dict_order()}.

%% @doc """
%% Starts the replica server with the given name and list of replicas.
%% """
-spec start(replica(), list(replica())) -> {ok, pid()} | {error, state()}.
start(Name, ListReplicas) ->
    %% Estado Inicial: {Data, ListReplicas, OrderData}
    gen_server:start_link({local, Name}, ?MODULE, {dict:new(), ListReplicas, dict:new()}, []).

-spec init(any()) -> {ok, state()}.
init(Args) ->
    {ok, Args}.

-spec terminate(any(), any()) -> ok.
terminate(_Reason, _Data) ->
    ok.

%% @doc """
%% Stops the replica server with the given name.
%% """
-spec stop(replica()) -> ok.
stop(Name) ->
    gen_server:cast(Name, stop).

%% @doc """
%% Gets the value associated with the given key from the replica server.
%% """
-spec get(key(), consistency(), replica()) -> get_reply().
get(Key, Consistency, Name) ->
    gen_server:call(Name, {get, Key, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%% @doc """
%% Deletes the value associated with the given key from the replica server.
%% """
-spec del(key(), timestamp(), consistency(), replica()) -> del_reply().
del(Key, Ts, Consistency, Name) ->
    gen_server:call(Name, {del, Key, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%% @doc """
%% Puts the given value associated with the given key into the replica server.
%% """
-spec put(key(), value(), timestamp(), consistency(), replica()) -> put_reply().
put(Key, Value, Ts, Consistency, Name) ->
    gen_server:call(Name, {put, Key, Value, Ts, Consistency}),
    receive
        {reply, Answer} -> Answer
    end.

%%%-------------------------------------------------------------------
%%% Replica Functions
%%%-------------------------------------------------------------------
-spec replica_get(pid_ref(), order_ref(), key(), replica()) -> ok.
replica_get(PidCoordinador, Ref, Key, Replica) ->
    gen_server:cast(Replica, {replica_get, PidCoordinador, Ref, Key}).

%% @doc """
%% Handles the put request from a replica.
%% """
-spec replica_put(pid_ref(), order_ref(), key(), value(), timestamp(), replica()) -> ok.
replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica) ->
    gen_server:cast(Replica, {replica_put, Key, Value, Ts, PidCoordinador, Ref}).

%% @doc """
%% Handles the delete request from a replica.
%% """
-spec replica_del(pid_ref(), order_ref(), key(), timestamp(), replica()) -> ok.
replica_del(PidCoordinador, Ref, Key, Ts, Replica) ->
    gen_server:cast(Replica, {replica_del, Key, Ts, PidCoordinador, Ref}).

%% @doc """
%% Fixes the replica for a given key.
%% """
-spec replica_fix(key(), get_reply(), replica()) ->
    ok.
replica_fix(Key, Value, Replica) ->
    gen_server:cast(Replica, {replica_fix, Key, Value}).

%%%-------------------------------------------------------------------
%%% Gen_server callbacks
%%%-------------------------------------------------------------------
-spec handle_call(any(), {pid(), any()}, state()) -> {reply, any(), state()}.
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
    {Pid, _} = From,
    NewOrderData = generate_order(Key, Pid, ListReplicas, Cons, OrderData, get),
    NewShinyData = new_order(Pid, {Key}, Data, Cons, ListReplicas, get),
    {reply, {wait}, {NewShinyData, ListReplicas, NewOrderData}}.

%% @doc """
%% Handles the cast messages for the gen_server.
%% """
-spec handle_cast(any(), state()) -> {noreply, state()}.
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

%% @doc """
%% Handles the info messages for the gen_server.
%% """
-spec handle_info(any(), state()) -> {noreply, state()}.
handle_info({fulfill_order, SenderPid, Ref, Value, SavedValue}, {Data, ReplicaList, OrderData}) ->
    case dict:find(Ref, OrderData) of
        {ok,
            Order = #order{
                op = Op,
                expected_responses = ExpectedResponses,
                responses = Responses,
                best_value = BestValue,
                key = Key
            }} ->
            NewBestValue = compare_values(Value, BestValue),
            NewData =
                case NewBestValue of
                    Value ->
                        Data;
                    _ ->
                        {_, ND} = put_value(Key, SavedValue, Data),
                        ND
                end,
            ensure_sender_consistency(SenderPid, Key, SavedValue, NewData),
            NewResponses = Responses + 1,
            NewOrderData =
                case NewResponses of
                    ExpectedResponses ->
                        Ref ! {reply, format_client_response(Op, NewBestValue)},
                        dict:erase(Ref, OrderData);
                    _ ->
                        OrderNew = Order#order{
                            responses = NewResponses,
                            best_value = NewBestValue
                        },
                        dict:store(Ref, OrderNew, OrderData)
                end,
            {noreply, {NewData, ReplicaList, NewOrderData}};
        _ ->
            {noreply, {Data, ReplicaList, OrderData}}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%% @doc """
%% Ensures consistency for the sender.
%% """
-spec ensure_sender_consistency(pid(), key(), get_reply(), dict_data()) -> get_reply().
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

%% @doc """
%% Generates a new order for the given reference.
%% """
-spec generate_order(
    key(), order_ref(), list(replica()), consistency(), dict_order(), operation()
) -> dict_order().
generate_order(Key, Ref, ListReplicas, Consistency, OrderData, Op) ->
    Order = #order{
        op = Op,
        expected_responses = get_expected_responses(length(ListReplicas), Consistency),
        responses = 0,
        best_value = {not_found},
        key = Key
    },
    dict:store(Ref, Order, OrderData).
%% @doc """
%% Gets the expected number of responses based on the consistency level.
%% """
-spec get_expected_responses(integer(), consistency()) -> response_qty().
get_expected_responses(Length, Consistency) ->
    case Consistency of
        one -> 1;
        quorum -> (Length) div 2 + 1;
        all -> Length + 1
    end.

%% @doc """
%% Creates a new order based on the consistency level.
%% """
-spec new_order(
    order_ref(), operation_data(), dict_data(), consistency(), list(replica()), operation()
) ->
    dict_data().
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

%% @doc """
%% Requests the fulfillment of an order.
%% """
-spec request_order_fullfilment(order_ref(), pid(), operation_data(), list(replica()), operation()) ->
    ok.
request_order_fullfilment(_, _, _, [], _) ->
    ok;
request_order_fullfilment(Ref, PidCoordinador, OpData, [Replica | Rest], Op) ->
    apply_replica_operation(Op, OpData, PidCoordinador, Ref, Replica),
    request_order_fullfilment(Ref, PidCoordinador, OpData, Rest, Op).

%% @doc """
%% Propagates the operation to the replicas.
%% """
-spec propagate_operation(operation(), operation_data(), list(replica())) -> ok.
propagate_operation(_, _, []) ->
    ok;
propagate_operation(Op, OpData, [Replica | Rest]) ->
    apply_replica_operation(Op, OpData, ?FAKE_PID, ?FAKE_PID, Replica),
    propagate_operation(Op, OpData, Rest).

%% @doc """
%% Compares two values and returns the best one.
%% """
-spec compare_values(operation_value(), operation_value()) -> operation_value().
compare_values({ok, _, BestTs} = BestValue, {ko, Ts}) when BestTs > Ts -> BestValue;
compare_values({ok, _, _}, {ko, _} = BestValue) -> BestValue;
compare_values({ok, _, BestTs} = BestValue, {ok, _, Ts}) when BestTs > Ts -> BestValue;
compare_values({ok, _, _}, {ok, _, _} = BestValue) -> BestValue;
compare_values({ko, _} = Arg2, {ok, _, _} = Arg1) -> compare_values(Arg1, Arg2);
compare_values({ko, BestTs}, {ko, Ts}) when BestTs > Ts -> {ko, BestTs};
compare_values({ko, _}, {ko, BestTs}) -> {ko, BestTs};
compare_values({not_found}, BestValue) -> BestValue;
compare_values(BestValue, {not_found}) -> BestValue.

%% @doc """
%% Applies the given operation to the data. Returns the best value and the new data.
%% """
-spec apply_operation(operation(), operation_data(), dict_data()) ->
    {operation_value(), dict_data()}.
apply_operation(get, {Key}, Data) ->
    BestValue = get_value(Key, Data),
    {BestValue, Data};
apply_operation(put, {Key, Value, Ts}, Data) ->
    put_value(Key, Value, Ts, Data);
apply_operation(del, {Key, Ts}, Data) ->
    delete_value(Key, Ts, Data).

%% @doc """
%% Applies the given operation to a replica, passing the order ref to fulfill.
%% """
-spec apply_replica_operation(operation(), operation_data(), pid_ref(), order_ref(), replica()) ->
    ok.
apply_replica_operation(get, {Key}, PidCoordinador, Ref, Replica) ->
    replica_get(PidCoordinador, Ref, Key, Replica);
apply_replica_operation(put, {Key, Value, Ts}, PidCoordinador, Ref, Replica) ->
    replica_put(PidCoordinador, Ref, Key, Value, Ts, Replica);
apply_replica_operation(del, {Key, Ts}, PidCoordinador, Ref, Replica) ->
    replica_del(PidCoordinador, Ref, Key, Ts, Replica).

%% @doc """
%% Gets the value associated with the given key from the data.
%% """
-spec get_value(key(), dict_data()) -> get_reply().
get_value(Key, Data) ->
    case dict:find(Key, Data) of
        {ok, {?DELETE_VALUE, Ts}} -> {ko, Ts};
        {ok, {Val, Ts}} -> {ok, Val, Ts};
        _ -> {not_found}
    end.

%% @doc """
%% Deletes the value associated with the given key from the data.
%% """
-spec delete_value(key(), timestamp(), dict_data()) -> {operation_value(), dict_data()}.
delete_value(Key, Ts, Data) ->
    case get_value(Key, Data) of
        {ko, NewTs} when NewTs > Ts -> {{not_found}, Data};
        {ko, _} -> {{not_found}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {ok, _, NewTs} when NewTs > Ts -> {{ko, NewTs}, Data};
        {ok, _, _} -> {{ok, Ts, Ts}, dict:store(Key, {?DELETE_VALUE, Ts}, Data)};
        {not_found} -> {{not_found}, Data}
    end.

%% @doc """
%% Puts the given value associated with the given key into the data.
%% """
-spec put_value(key(), value(), timestamp(), dict_data()) -> {operation_value(), dict_data()}.
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

%% @doc """
%% Formats the client response based on the operation and result.
%% """
-spec format_client_response(operation(), operation_value()) ->
    get_reply() | put_reply() | del_reply().
format_client_response(get, Value) ->
    Value;
format_client_response(_, {ok, _, _}) ->
    {ok};
format_client_response(_, {ko, _}) ->
    {ko};
format_client_response(_, RES) ->
    RES.
