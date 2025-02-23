-module(test_base_de_datos).
-include_lib("eunit/include/eunit.hrl").

% Test setup and teardown
setup_test_() ->
    {setup, fun setup/0, fun cleanup/0}.

setup() ->
    ok.

cleanup() ->
    ok.
start_bdd()->
    process_flag(trap_exit, true),
    Int = integer_to_list(erlang:unique_integer([monotonic])),
    Name = "bdd" ++ Int,
    base_de_datos:start(list_to_atom(Name), 10),
    Name.
stop_bdd(Name) ->
    base_de_datos:stop(list_to_atom(Name)).

replica_atom(Name,Int)->
    list_to_atom(Name ++ "_" ++ Int).

put_get_test() ->
    Name = start_bdd(),
    Key = key,
    Value = value,
    Ts = get_ts(),
    ?assertEqual({ok}, replica:put(Key, Value, Ts, quorum, replica_atom(Name, "1"))),
    ?assertEqual({ok, Value, Ts}, replica:get(Key, one,  replica_atom(Name, "5"))),
    stop_bdd(Name).

del_get_test() ->
    Name = start_bdd(),
    Key = key,
    Value = value,
    Ts = get_ts(),
    Bdd1 = replica_atom(Name, "1"),
    Bdd5 = replica_atom(Name, "5"),
    ?assertEqual({ok}, replica:put(Key, Value, Ts, quorum, Bdd1)),
    ?assertEqual({ok}, replica:del(Key, Ts, all, Bdd5)),
    ?assertEqual({ko, Ts}, replica:get(Key, one, Bdd5)).
consistency_test() ->
    Name = start_bdd(),
    Key = key,
    Value = value,
    Ts = get_ts(),
    ?assertEqual({ok}, replica:put(Key, Value, Ts, all, replica_atom(Name, "1"))),
    [?assertEqual({ok, Value, Ts}, replica:get(Key, one, replica_atom(Name, integer_to_list(Append)))) || Append <- lists:seq(2, 9)],
    stop_bdd(Name).
fix_consistency_test() ->
    Name = start_bdd(),
    Bdd1 = replica_atom(Name, "1"),
    Bdd2 = replica_atom(Name, "2"),
    Key = key,
    Value = value,
    Ts = get_ts(),
    ?assertEqual({ok}, replica:put(Key, Value, Ts, quorum, Bdd1)),
    replica:stop(bdd_2),
    %Eventually consistent
    timer:sleep(1),
    ?assertEqual({ok, Value, Ts}, replica:get(Key, quorum, Bdd1)),
    ?assertEqual({ok, Value, Ts}, replica:get(Key, one, Bdd2)),
    ?assertEqual({ok}, replica:del(Key, Ts, all, Bdd1)),
    stop_bdd(Name).
maintain_consistency_test() ->
    NameBdd = start_bdd(),
    Bdd1 = replica_atom(NameBdd, "1"),
    Bdd2 = replica_atom(NameBdd, "2"),
    Key = key,
    Value = value,
    Ts = get_ts(),
    ?assertEqual({ok}, replica:put(Key, Value, Ts, quorum, Bdd1)),
    [
        replica:stop(list_to_atom(Name ++ integer_to_list(Append)))
     || Name <- [NameBdd ++ "_"], Append <- lists:seq(1, 4)
    ],
    timer:sleep(1),
    ?assertEqual({ok, Value, Ts}, replica:get(Key, quorum, Bdd1)),
    ?assertEqual({ok}, replica:del(Key, Ts, all, Bdd2)),
    stop_bdd(NameBdd).

get_ts() ->
    {Mega, Sec, Micro} = os:timestamp(),
    Mega * 1000000 * 1000000 + Sec * 1000000 + Micro.
