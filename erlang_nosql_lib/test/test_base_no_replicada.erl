-module(test_base_no_replicada).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    {ok, Pid} = base_no_replicada:start(),
    ?assert(is_pid(Pid)),
    base_no_replicada:stop().

stop_test() ->
    {ok, Pid} = base_no_replicada:start(),
    base_no_replicada:stop(),
    ?assert(not is_process_alive(Pid)).

put_get_test() ->
    {ok, _} = base_no_replicada:start(),
    Key = <<"key">>,
    Value = <<"value">>,
    Ts = 1,
    ?assertEqual({ok}, base_no_replicada:put(Key, Value, Ts)),
    ?assertEqual({ok, Value, Ts}, base_no_replicada:get(Key)),
    base_no_replicada:stop().

del_test() ->
    {ok, _} = base_no_replicada:start(),
    Key = <<"key">>,
    Value = <<"value">>,
    Ts = 1,
    base_no_replicada:put(Key, Value, Ts),
    ?assertEqual({ok}, base_no_replicada:del(Key, Ts)),
    ?assertEqual({ko, Ts}, base_no_replicada:get(Key)),
    base_no_replicada:stop().

size_test() ->
    {ok, _} = base_no_replicada:start(),
    Key1 = <<"key1">>,
    Key2 = <<"key2">>,
    Value = <<"value">>,
    Ts = 1,
    base_no_replicada:put(Key1, Value, Ts),
    base_no_replicada:put(Key2, Value, Ts),
    ?assertEqual(2, base_no_replicada:size()),
    base_no_replicada:del(Key1, Ts),
    ?assertEqual(1, base_no_replicada:size()),
    base_no_replicada:stop().
