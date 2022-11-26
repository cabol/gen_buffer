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

-spec start_link(gen_buffer:t(), gen_buffer:opts()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Buffer, Opts) when is_list(Opts) ->
  start_link(Buffer, maps:from_list(Opts));

start_link(Buffer, Opts) when is_map(Opts) ->
  case supervisor:start_link({local, Buffer}, ?MODULE, {Buffer, Opts}) of
    {ok, Pid} = Ok ->
      ok = init_pg(Buffer, Pid),
      ok = start_children(Buffer, Opts),
      Ok;

    Error ->
      Error
  end.

-spec start_child(gen_buffer:t(), gen_buffer:opts()) -> supervisor:startchild_ret().
start_child(Buffer, Opts) when is_list(Opts) ->
  start_child(Buffer, maps:from_list(Opts));

start_child(Buffer, Opts) when is_map(Opts) ->
  supervisor:start_child(Buffer, [Opts#{buffer => Buffer}]).

-spec terminate_child(gen_buffer:t()) -> ok | {error, not_found | simple_one_for_one}.
terminate_child(Buffer) ->
  case whereis(Buffer) of
    undefined ->
      ok;
    _ ->
      case gen_buffer:get_worker(Buffer) of
        {ok, Worker} ->
          supervisor:terminate_child(Buffer, Worker);

        Error ->
          Error
      end
  end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init({Buffer, Opts}) ->
  _ = create_buffer(Buffer, Opts),
  Children = [child(?WORKER, [Opts#{buffer => Buffer}])],
  supervise(Children, #{strategy => simple_one_for_one}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
child(Module, Args) ->
  #{
    id    => Module,
    start => {Module, start_link, Args}
  }.

%% @private
supervise(Children, SupFlagsMap) ->
  Strategy = maps:get(strategy, SupFlagsMap, one_for_one),
  Intensity = maps:get(intensity, SupFlagsMap, 10),
  Period = maps:get(period, SupFlagsMap, 10),
  {ok, {{Strategy, Intensity, Period}, Children}}.

create_buffer(Buffer, Opts) ->
  NumPartitions = maps:get(n_partitions, Opts, 1),
  create_buffer(Buffer, Opts, NumPartitions).

%% @private
create_buffer(Buffer, #{buffer_type := ring} = Opts, NumPartitions) ->
  Size = maps:get(buffer_size, Opts, 100),
  create_partitioned_buffer(Buffer, [ring, Size], NumPartitions);

create_buffer(Buffer, #{buffer_type := lifo}, NumPartitions) ->
  create_partitioned_buffer(Buffer, [lifo], NumPartitions);

create_buffer(Buffer, _, NumPartitions) ->
  create_partitioned_buffer(Buffer, [fifo], NumPartitions).

%% @private
create_partitioned_buffer(Buffer, Args, NumPartitions) ->
  % create metadata table
  Buffer = create_metadata(Buffer),

  % store number of partitions
  true = ets:insert(Buffer, {n_partitions, NumPartitions}),

  % create partitions (one buffer per partition)
  Partitions = [begin
    PartitionName = gen_buffer_lib:partition_name(Buffer, Partition),
    _ = apply(ets_buffer, create_dedicated, [PartitionName | Args]),
    PartitionName
  end || Partition <- lists:seq(0, NumPartitions - 1)],

  % store partition names
  true = ets:insert(Buffer, {partitions, Partitions}),
  ok.

%% @private
create_metadata(Buffer) ->
  ets:new(Buffer, [
    named_table,
    public,
    duplicate_bag,
    {read_concurrency, true},
    {write_concurrency, true}
  ]).

%% @private
start_children(Buffer, Opts) ->
  lists:foreach(fun(_) ->
    case start_child(Buffer, Opts) of
      {ok, Child} when is_pid(Child); Child =:= undefined ->
        ok;

      {error, Error} ->
        exit(whereis(Buffer), Error)
    end
  end, lists:seq(1, maps:get(workers, Opts, ?DEFAULT_WORKERS))).

%% @private
init_pg(Buffer, SupPid) ->
  ok = gen_buffer_pg:create(Buffer),
  ok = gen_buffer_pg:join(Buffer, SupPid),
  ok.
