%%%-------------------------------------------------------------------
%%% @doc
%%% gen_buffer supervisor.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer_sup).

-behaviour(supervisor).

%% API
-export([
  start_link/2,
  start_child/2,
  terminate_child/1
]).

%% Supervisor callbacks
-export([init/1]).

%% Buffer worker module
-define(WORKER, gen_buffer_worker).

%% Default number of workers
-define(DEFAULT_WORKERS, erlang:system_info(schedulers_online)).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link(
        Channel :: gen_buffer:channel(),
        Opts    :: gen_buffer:opts()
      ) -> {ok, pid()} | ignore | {error, term()}.
start_link(Channel, Opts) when is_list(Opts) ->
  start_link(Channel, maps:from_list(Opts));

start_link(Channel, Opts) when is_map(Opts) ->
  case supervisor:start_link({local, Channel}, ?MODULE, {Channel, Opts}) of
    {ok, Pid} = Ok ->
      ok = init_pg2(Channel, Pid),
      ok = start_children(Channel, Opts),
      Ok;

    Error ->
      Error
  end.

-spec start_child(
        Channel :: gen_buffer:channel(),
        Opts    :: gen_buffer:opts()
      ) -> supervisor:startchild_ret().
start_child(Channel, Opts) when is_list(Opts) ->
  start_child(Channel, maps:from_list(Opts));

start_child(Channel, Opts) when is_map(Opts) ->
  supervisor:start_child(Channel, [Opts#{channel => Channel}]).

-spec terminate_child(
        Channel :: gen_buffer:channel()
      ) -> ok | {error, not_found | simple_one_for_one}.
terminate_child(Channel) ->
  case whereis(Channel) of
    undefined ->
      ok;
    _ ->
      case gen_buffer:get_worker(Channel) of
        {ok, Worker} ->
          supervisor:terminate_child(Channel, Worker);

        Error ->
          Error
      end
  end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Channel, Opts}) ->
  _ = create_buffer(Channel, Opts),
  Children = [child(?WORKER, [Opts#{channel => Channel}])],
  supervise(Children, #{strategy => simple_one_for_one}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Module, Args) ->
  #{
    id      => gen_buffer_lib:concat(?WORKER, erlang:phash2(os:timestamp())),
    start   => {Module, start_link, Args},
    restart => permanent
  }.

%% @private
supervise(Children, SupFlagsMap) ->
  Strategy = maps:get(strategy, SupFlagsMap, one_for_one),
  Intensity = maps:get(intensity, SupFlagsMap, 10),
  Period = maps:get(period, SupFlagsMap, 10),
  {ok, {{Strategy, Intensity, Period}, Children}}.

create_buffer(Channel, Opts) ->
  NumPartitions = maps:get(n_partitions, Opts, 1),
  create_buffer(Channel, Opts, NumPartitions).

%% @private
create_buffer(Channel, #{buffer_type := ring} = Opts, NumPartitions) ->
  Size = maps:get(buffer_size, Opts, 100),
  create_partitioned_buffer(Channel, [ring, Size], NumPartitions);

create_buffer(Channel, #{buffer_type := lifo}, NumPartitions) ->
  create_partitioned_buffer(Channel, [lifo], NumPartitions);

create_buffer(Channel, _, NumPartitions) ->
  create_partitioned_buffer(Channel, [fifo], NumPartitions).

%% @private
create_partitioned_buffer(Channel, Args, NumPartitions) ->
  % create metadata table
  Channel = create_metadata(Channel),

  % store number of partitions
  true = ets:insert(Channel, {n_partitions, NumPartitions}),

  % create partitions (one buffer per partition)
  Partitions = [begin
    PartitionName = gen_buffer_lib:partition_name(Channel, Partition),
    _ = apply(ets_buffer, create_dedicated, [PartitionName | Args]),
    PartitionName
  end || Partition <- lists:seq(0, NumPartitions - 1)],

  % store partition names
  true = ets:insert(Channel, {partitions, Partitions}),
  ok.

%% @private
create_metadata(Channel) ->
  ets:new(Channel, [
    named_table,
    public,
    duplicate_bag,
    {read_concurrency, true},
    {write_concurrency, true}
  ]).

%% @private
start_children(Channel, Opts) ->
  lists:foreach(fun(_) ->
    case start_child(Channel, Opts) of
      {ok, Child} when is_pid(Child); Child =:= undefined ->
        ok;

      {error, Error} ->
        exit(whereis(Channel), Error)
    end
  end, lists:seq(1, maps:get(workers, Opts, ?DEFAULT_WORKERS))).

%% @private
init_pg2(Channel, SupPid) ->
  Group = gen_buffer:pg2_namespace(Channel),
  ok = pg2:create(Group),
  ok = pg2:join(Group, SupPid),
  ok.
