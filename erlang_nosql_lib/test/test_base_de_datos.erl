-module(test_base_de_datos).
-include_lib("eunit/include/eunit.hrl").

% Test setup and teardown
setup(Name, Replicas) ->
    base_de_datos:start(Name, Replicas).

cleanup(_) ->
    base_de_datos:stop().

% Helper function to wait for replies
wait_for_reply() ->
    receive
        {reply, Result} -> Result
    after 1000 -> timeout
    end.

% Test cases
basic_put_get_one_test_() ->
    {setup,
     fun() -> setup("test_put_get_one", 1) end,
     fun cleanup/1,
     [fun() ->
        Replica = list_to_atom("test_put_get_one_1"),
        Key = key1,
        Value = value1,
        Ts = 1,

        % Test PUT
        ?assertEqual({wait}, replica:put(Replica, Key, Value, Ts, one)),
        ?assertEqual({ok, Value, Ts}, wait_for_reply()),

        % Test GET
        ?assertEqual({wait}, replica:get(Replica, Key, one)),
        ?assertEqual({ok, Value, Ts}, wait_for_reply())
     end]}.

delete_one_test_() ->
    {setup,
     fun() -> setup("test_delete_one", 1) end,
     fun cleanup/1,
     [fun() ->
        Replica = list_to_atom("test_delete_one_1"),
        Key = key1,
        Value = value1,
        TsPut = 1,
        TsDel = 2,

        % PUT
        ?assertEqual({wait}, replica:put(Replica, Key, Value, TsPut, one)),
        ?assertEqual({ok, Value, TsPut}, wait_for_reply()),

        % DELETE
        ?assertEqual({wait}, replica:del(Replica, Key, TsDel, one)),
        ?assertEqual({ok, TsDel, TsDel}, wait_for_reply()),

        % GET after DELETE
        ?assertEqual({wait}, replica:get(Replica, Key, one)),
        ?assertEqual({ko, TsDel}, wait_for_reply())
     end]}.

quorum_conflict_test_() ->
    {setup,
     fun() -> setup("test_quorum_conflict", 3) end,
     fun cleanup/1,
     [fun() ->
        Replica1 = list_to_atom("test_quorum_conflict_1"),
        Replica3 = list_to_atom("test_quorum_conflict_3"),
        Key = key1,
        Value1 = value1,
        Value2 = value2,
        Ts1 = 1,
        Ts2 = 2,

        % PUT with quorum via Replica1
        ?assertEqual({wait}, replica:put(Replica1, Key, Value1, Ts1, quorum)),
        ?assertEqual({ok, Value1, Ts1}, wait_for_reply()),

        % PUT with quorum via Replica3 (higher timestamp)
        ?assertEqual({wait}, replica:put(Replica3, Key, Value2, Ts2, quorum)),
        ?assertEqual({ok, Value2, Ts2}, wait_for_reply()),

        % GET with quorum via Replica1 (should return Value2)
        ?assertEqual({wait}, replica:get(Replica1, Key, quorum)),
        ?assertEqual({ok, Value2, Ts2}, wait_for_reply())
     end]}.

all_consistency_test_() ->
    {setup,
     fun() -> setup("test_all", 3) end,
     fun cleanup/1,
     [fun() ->
        Replica = list_to_atom("test_all_1"),
        Key = key1,
        Value = value1,
        Ts = 1,

        % PUT with all consistency
        ?assertEqual({wait}, replica:put(Replica, Key, Value, Ts, all)),
        ?assertEqual({ok, Value, Ts}, wait_for_reply()),

        % Verify all replicas have the value
        lists:foreach(
            fun(N) ->
                R = list_to_atom("test_all_" ++ integer_to_list(N)),
                ?assertEqual({wait}, replica:get(R, Key, one)),
                ?assertEqual({ok, Value, Ts}, wait_for_reply())
            end,
            [1, 2, 3]
        )
     end]}.

conflict_resolution_test_() ->
    {setup,
     fun() -> setup("test_conflict", 3) end,
     fun cleanup/1,
     [fun() ->
        Replica1 = list_to_atom("test_conflict_1"),
        Replica2 = list_to_atom("test_conflict_2"),
        Key = key1,
        Value1 = value1,
        Value2 = value2,
        Ts1 = 1,
        Ts2 = 2,

        % PUT with one consistency to Replica1
        ?assertEqual({wait}, replica:put(Replica1, Key, Value1, Ts1, one)),
        ?assertEqual({ok, Value1, Ts1}, wait_for_reply()),

        % PUT with one consistency to Replica2 (higher timestamp)
        ?assertEqual({wait}, replica:put(Replica2, Key, Value2, Ts2, one)),
        ?assertEqual({ok, Value2, Ts2}, wait_for_reply()),

        % GET with quorum (should return Value2)
        ?assertEqual({wait}, replica:get(Replica1, Key, quorum)),
        ?assertEqual({ok, Value2, Ts2}, wait_for_reply())
     end]}.

stop_test_() ->
    {setup,
     fun() -> setup("test_stop", 1) end,
     fun cleanup/1,
     [fun() ->
        Replica = list_to_atom("test_stop_1"),
        ?assertEqual(ok, base_de_datos:stop()),
        % Verify replica is stopped
        ?assertException(exit, _, replica:get(Replica, key1, one))
     end]}.