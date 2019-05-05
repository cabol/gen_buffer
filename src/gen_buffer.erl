%%%-------------------------------------------------------------------
%%% @doc
%%% gen_buffer.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer).

%% API
-export([
  start_link/2,
  stop/1,
  child_spec/1,
  eval/2,
  eval/3,
  send/2,
  send/3,
  poll/1,
  recv/2,
  recv/3,
  sync_send_recv/2,
  sync_send_recv/3,
  get_worker/1,
  get_workers/1,
  incr_worker/1,
  incr_worker/2,
  decr_worker/1,
  set_workers/2,
  set_workers/3,
  queue_size/1,
  info/1,
  info/0
]).

%% Global Utilities
-export([
  pg2_namespace/1
]).

%%%===================================================================
%%% Types
%%%===================================================================

-type channel() :: atom().

-type message() :: any().

-type msg_handler_fun() :: fun((channel(), message()) -> any()).

-type err_handler_fun() :: fun((channel(), message(), Error :: any()) -> any()).

-type opt() :: {channel, atom()}
             | {message_handler, module()}
             | {workers, non_neg_integer()}
             | {buffer_type, ring | fifo | lifo}
             | {buffer_size, non_neg_integer()}
             | {send_replies, boolean()}
             | {init_args, any()}.

-type opts_map() :: #{
  channel         => atom(),
  message_handler => module(),
  workers         => non_neg_integer(),
  buffer_type     => ring | fifo | lifo,
  buffer_size     => non_neg_integer(),
  send_replies    => boolean(),
  init_args       => any()
}.

-type opts() :: opts_map() | [opt()].

-type channel_info() :: #{
  workers    := non_neg_integer(),
  queue_size := non_neg_integer()
}.

-type channels_info() :: #{
  channel() => channel_info()
}.

-export_type([
  channel/0,
  message/0,
  msg_handler_fun/0,
  err_handler_fun/0,
  opts/0,
  opts_map/0,
  channel_info/0,
  channels_info/0
]).

-type init_res() :: {ok, State :: any()}
                  | {ok, State :: any(), Timeout :: timeout()}
                  | {ok, State :: any(), hibernate}
                  | {stop, Reason :: any()}
                  | ignore.

-type handle_info_res() :: {noreply, NewState :: any()}
                         | {noreply, NewState :: any(), Timeout :: timeout()}
                         | {noreply, NewState :: any(), hibernate}
                         | {stop, Reason :: normal | any(), NewState :: any()}.

-type terminate_reason() :: normal | shutdown | {shutdown, any()} | any().

%%%===================================================================
%%% Callbacks
%%%===================================================================

-optional_callbacks([
  init/1,
  handle_info/3,
  terminate/3
]).

-callback init(Args :: any()) -> init_res().

-callback handle_message(
            Channel :: gen_buffer:channel(),
            Message :: gen_buffer:message(),
            State   :: any()
          ) -> {ok, Reply :: any(), NewState :: any()}.

-callback handle_info(
            Channel :: gen_buffer:channel(),
            Info    :: any(),
            State   :: any()
          ) -> handle_info_res().

-callback terminate(
            Channel :: gen_buffer:channel(),
            Reason  :: terminate_reason(),
            State   :: any()
          ) -> any().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(
        Channel :: channel(),
        Opts    :: opts()
      ) -> {ok, pid()} | {error, term()}.
start_link(Channel, Opts) ->
  gen_buffer_sup:start_link(Channel, Opts).

-spec stop(Channel :: channel()) -> ok.
stop(Channel) ->
  case whereis(Channel) of
    undefined -> ok;
    Pid       -> gen:stop(Pid, normal, infinity)
  end.

-spec child_spec(Opts :: opts()) -> supervisor:child_spec().
child_spec(Opts) when is_list(Opts) ->
  child_spec(maps:from_list(Opts));

child_spec(Opts) when is_map(Opts) ->
  Channel = maps:get(channel, Opts),

  #{
    id      => Channel,
    start   => {gen_buffer_sup, start_link, [Channel, Opts]},
    restart => permanent
  }.

%% @equiv eval(Channel, Message, 1)
eval(Channel, Message) ->
  eval(Channel, Message, 1).

-spec eval(
        Channel :: channel(),
        Message :: any(),
        Retries :: non_neg_integer()
      ) -> any().
eval(Channel, Message, Retries) when is_integer(Retries); Retries >= 0 ->
  case gen_buffer_lib:get_available_worker(Channel) of
    no_available_workers ->
      maybe_retry(Channel, Message, Retries - 1);

    Worker ->
      gen_buffer_worker:eval(Worker, Message)
  end.

%% @equiv send(Channel, Message, self())
send(Channel, Message) ->
  send(Channel, Message, self()).

-spec send(
        Channel :: channel(),
        Message :: any(),
        ReplyTo :: pid() | atom() | {atom(), node()}
      ) -> reference().
send(Channel, Message, ReplyTo) ->
  Ref = make_ref(),
  WorkerMessage = {ReplyTo, Ref, Message},
  case gen_buffer_lib:get_available_worker(Channel) of
    no_available_workers ->
      ChannelPartition = gen_buffer_lib:get_partition(Channel),
      _ = ets_buffer:write_dedicated(ChannelPartition, WorkerMessage),
      Ref;

    Worker ->
      ok = gen_buffer_worker:send(Worker, WorkerMessage),
      Ref
  end.

-spec poll(Channel :: channel()) -> ok | {error, term()}.
poll(Channel) when is_atom(Channel) ->
  case get_worker(Channel) of
    {ok, Worker} ->
      gen_buffer_worker:poll(Worker);

    Error ->
      Error
  end.

%% @equiv recv(Channel, Ref, infinity)
recv(Channel, Ref) ->
  recv(Channel, Ref, infinity).

-spec recv(
        Channel :: channel(),
        Ref     :: reference(),
        Timeout :: timeout()
      ) -> {ok, HandlerResponse :: any()} | {error, Reason :: any()}.
recv(Channel, Ref, Timeout) ->
  receive
    {reply, Ref, Channel, Payload} ->
      {ok, Payload};

    {error, Ref, Channel, Error} ->
      {error, Error}
  after
    Timeout -> {error, timeout}
  end.

%% @equiv sync_send_recv(Channel, Message, infinity)
sync_send_recv(Channel, Message) ->
  sync_send_recv(Channel, Message, infinity).

-spec sync_send_recv(
        Channel :: channel(),
        Message :: any(),
        Timeout :: timeout()
      ) -> {ok, HandlerResponse :: any()} | {error, Reason :: any()}.
sync_send_recv(Channel, Message, Timeout) ->
  Ref = send(Channel, Message),
  recv(Channel, Ref, Timeout).

-spec get_worker(Channel :: channel()) -> {ok, pid()} | {error, term()}.
get_worker(Channel) ->
  case get_children(Channel) of
    {ok, Children} ->
      Nth = erlang:phash2(os:timestamp(), length(Children)) + 1,
      {_, Pid, _, _} = lists:nth(Nth, Children),
      {ok, Pid};

    Error ->
      Error
  end.

-spec get_workers(Channel :: channel()) -> {ok, [pid()]} | {error, term()}.
get_workers(Channel) ->
  case get_children(Channel) of
    {ok, Children} ->
      {ok, [Pid || {_, Pid, _, _} <- Children]};

    Error ->
      Error
  end.

%% @equiv incr_worker(Channel, #{})
incr_worker(Channel) ->
  incr_worker(Channel, #{}).

-spec incr_worker(
        Channel :: channel(),
        Opts    :: opts()
      ) -> supervisor:startchild_ret().
incr_worker(Channel, Opts) ->
  gen_buffer_sup:start_child(Channel, Opts).

-spec decr_worker(Channel :: channel()) -> ok.
decr_worker(Channel) ->
  gen_buffer_sup:terminate_child(Channel).

%% @equiv set_workers(Channel, N, #{})
set_workers(Channel, N) ->
  set_workers(Channel, N, #{}).

-spec set_workers(
        Channel :: channel(),
        N       :: non_neg_integer(),
        Opts    :: opts()
      ) -> {ok, [pid()]} | {error, term()}.
set_workers(Channel, N, Opts) when N > 0 ->
  try
    NumWorkers =
      case get_workers(Channel) of
        {ok, Workers} ->
          length(Workers);

        {error, no_available_workers} ->
          0;

        Error ->
          throw(Error)
      end,

    {Fun, Diff} =
      case N >= NumWorkers of
        true  -> {fun incr_worker/2, N - NumWorkers};
        false -> {fun decr_worker/1, NumWorkers - N}
      end,

    ok = lists:foreach(fun
      (_) when is_function(Fun, 2) -> Fun(Channel, Opts);
      (_) when is_function(Fun, 1) -> Fun(Channel)
    end, lists:seq(1, Diff)),

    get_workers(Channel)
  catch
    throw:Ex -> Ex
  end.

-spec queue_size(Channel :: channel()) -> integer() | undefined.
queue_size(Channel) ->
  case whereis(Channel) of
    undefined ->
      undefined;

    _ ->
      % we have to subtract 1 because ets_buffer uses one entry for metadata
      Partitions = gen_buffer_lib:get_one_metadata_value(Channel, partitions, []),
      lists:foldl(fun(Partition, Acc) ->
        Acc + (ets:info(Partition, size) - 1)
      end, 0, Partitions)
  end.

-spec info(Channel :: channel()) -> channel_info() | undefined.
info(Channel) ->
  channel_info(Channel).

-spec info() -> channels_info().
info() ->
  lists:foldl(fun
    ({gen_buffer, Channel}, Acc) ->
      case channel_info(Channel) of
        undefined -> Acc;
        InfoChann -> Acc#{Channel => InfoChann}
      end;

    (_, Acc) ->
      Acc
  end, #{}, pg2:which_groups()).

%%%===================================================================
%%% Global Utilities
%%%===================================================================

-spec pg2_namespace(gen_buffer:channel()) -> any().
pg2_namespace(Channel) ->
  {gen_buffer, Channel}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
get_children(undefined) ->
  {error, no_available_channel};

get_children(Channel) when is_atom(Channel) ->
  get_children(whereis(Channel));

get_children(Channel) when is_pid(Channel) ->
  case supervisor:which_children(Channel) of
    []       -> {error, no_available_workers};
    Children -> {ok, Children}
  end.

%% @private
maybe_retry(_Channel, _Message, 0) ->
  no_available_workers;

maybe_retry(Channel, Message, Retries) when Retries < 0 ->
  eval(Channel, Message, 0);

maybe_retry(Channel, Message, Retries) ->
  eval(Channel, Message, Retries - 1).

%% @private
channel_info(Channel) ->
  case get_workers(Channel) of
    {ok, WorkerList} ->
      info_map(length(WorkerList), queue_size(Channel));

    {error, no_available_workers} ->
      info_map(0, queue_size(Channel));

    {error, no_available_channel} ->
      undefined
  end.

%% @private
info_map(Workers, QueueSize) ->
  #{
    workers    => Workers,
    queue_size => QueueSize
  }.
