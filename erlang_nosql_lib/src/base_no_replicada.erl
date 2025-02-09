-module(base_no_replicada).
-behaviour(gen_server).
-define(SERVER, diccionario).
-define(DELETE_VALUE, n2FlOTg0OWYtY2E4Zi00NjBhLTljNjgtYjQzNzQ1ZjYyZjAw).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([start/0, stop/0, put/3, get/1, del/2, size/0]).

start() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, dict:new(), []).

init(Args) ->
    {ok, Args}.
terminate(_Reason, _Data) ->
    ok.
stop() ->
    gen_server:cast(?SERVER, stop).
size() ->
    gen_server:call(?SERVER, size).
get(Key) ->
    gen_server:call(?SERVER, {get, Key}).
del(Key, Ts) ->
    gen_server:call(?SERVER, {del, Key, Ts}).
put(Key, Value, Ts) ->
    gen_server:call(?SERVER, {put, Key, Value, Ts}).

handle_call({get, Key}, _From, Data) ->
    Reply = get_value(Key, Data),
    {reply, Reply, Data};
handle_call({put, Key, Value, Ts}, _From, Data) ->
    case get_value(Key, Data) of
        {ok, _, OldTs} when OldTs > Ts -> {reply, {ko}, Data};
        {ko,OldTs} when OldTs > Ts -> {reply, {not_found}, Data};
        _ -> {reply, {ok}, dict:store(Key, {Value, Ts}, Data)}
    end;
handle_call({del, Key, Ts}, _From, Data) ->
    case get_value(Key, Data) of
        {ko,OldTs} when OldTs > Ts-> {reply, {not_found}, Data};
        {ko,_} -> {reply, {not_found}, delete_value(Key, Ts, Data)};
        {ok, _, OldTs} when OldTs > Ts -> {reply, {ko}, Data};
        {ok, _, _} -> {reply, {ok}, delete_value(Key, Ts, Data)};
        {not_found} -> {reply, {not_found}, delete_value(Key, Ts, Data)}
    end;
handle_call(size, _From, Data) ->
    {reply, count_if_not_deleted_value(Data), Data}.

handle_cast(stop, Data) ->
    {stop, normal, Data}.

handle_info(_Info, Data) ->
    {noreply, Data}.

get_value(Key, Data) ->
    case dict:find(Key, Data) of
        {ok, {?DELETE_VALUE, Ts}} -> {ko,Ts};
        {ok, {Val, Ts}} -> {ok, Val, Ts};
        _ -> {not_found}
    end.
delete_value(Key, Ts, Data) ->
    dict:store(Key, {?DELETE_VALUE, Ts}, Data).

count_if_not_deleted_value(Data) ->
    dict:fold(
        fun
            (_Key, {?DELETE_VALUE, _}, Acc) -> Acc;
            (_Key, _, Acc) -> Acc + 1
        end,
        0,
        Data
    ).
