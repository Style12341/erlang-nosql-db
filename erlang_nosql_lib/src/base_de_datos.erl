%%%% filepath: /g:/Other computers/My Computer/DriveSynced/UTN/Programacion Concurrente/Erlang/erlang-nosql-db/erlang_nosql_lib/src/base_de_datos.erl
-module(base_de_datos).
-behaviour(supervisor).

%% API
-export([start_link/2, stop/0]).

%% Supervisor callbacks
-export([init/1]).

%% Utility functions used in child specs
-export([generate_replicas_names/3, generate_correct_arguments/2]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------
start_link(Name, CantReplicas) ->
    %% Register the supervisor as 'nosql_db'
    supervisor:start_link({local, nosql_db}, ?MODULE, {Name, CantReplicas}).

stop() ->
    supervisor:stop(nosql_db).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------
init({SupName, CantReplicas}) ->
    %% Generate a list of replica names
    Names = generate_replicas_names(SupName, CantReplicas, []),
    %% Build a child spec for each replica. Each replica receives its own Name and a list of other replicas.
    ChildSpecs = [child_spec(RepName, Names) || RepName <- Names],
    %% one_for_one: if a child crashes, only that child is restarted.
    {ok, {{one_for_one, 5, 10}, ChildSpecs}}.

child_spec(Name, ListNames) ->
    %% Compute the correct arguments: [Name, ListOfOtherReplicas]
    CorrectArgs = generate_correct_arguments(Name, ListNames),
    %% The child spec will call replica:start_link/2 with a permanent restart type
    {Name, {replica, start, CorrectArgs}, permanent, 5000, worker, [replica]}.

%%--------------------------------------------------------------------
%% Replica name utilities
%%--------------------------------------------------------------------
generate_replicas_names(_, 0, Names) ->
    Names;
generate_replicas_names(BaseName, CantReplicas, Names) when CantReplicas > 0 ->
    NewName = list_to_atom(BaseName ++ "_" ++ integer_to_list(CantReplicas)),
    generate_replicas_names(BaseName, CantReplicas - 1, [NewName | Names]).

generate_correct_arguments(Name, ListNames) ->
    %% Remove the current name from the full list
    NewListNames = lists:subtract(ListNames, [Name]),
    [Name, NewListNames].
