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
  send_recv/2,
  send_recv/3,
  get_worker/1,
  get_workers/1,
  incr_worker/1,
  incr_worker/2,
  decr_worker/1,
  set_workers/2,
  set_workers/3,
  size/1,
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

-type t() :: atom().

-type message() :: any().

-type opt() :: {buffer, atom()}
             | {message_handler, module()}
             | {workers, non_neg_integer()}
             | {buffer_type, ring | fifo | lifo}
             | {buffer_size, non_neg_integer()}
             | {send_replies, boolean()}
             | {init_args, any()}.

-type opts_map() :: #{
  buffer          => atom(),
  message_handler => module(),
  workers         => non_neg_integer(),
  buffer_type     => ring | fifo | lifo,
  buffer_size     => non_neg_integer(),
  send_replies    => boolean(),
  init_args       => any()
}.

-type opts() :: opts_map() | [opt()].

-type buffer_info() :: #{
  workers := non_neg_integer(),
  size    := non_neg_integer()
}.

-type buffers_info() :: #{
  t() => buffer_info()
}.

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

-export_type([
  t/0,
  message/0,
  opts/0,
  opts_map/0,
  buffer_info/0,
  buffers_info/0
]).

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
            Buffer  :: gen_buffer:t(),
            Message :: gen_buffer:message(),
            State   :: any()
          ) -> {ok, Reply :: any(), NewState :: any()}.

-callback handle_info(
            Buffer :: gen_buffer:t(),
            Info   :: any(),
            State  :: any()
          ) -> handle_info_res().

-callback terminate(
            Buffer :: gen_buffer:t(),
            Reason :: terminate_reason(),
            State  :: any()
          ) -> any().

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(
        Buffer :: t(),
        Opts   :: opts()
      ) -> {ok, pid()} | {error, term()}.
start_link(Buffer, Opts) ->
  gen_buffer_sup:start_link(Buffer, Opts).

-spec stop(Buffer :: t()) -> ok.
stop(Buffer) ->
  case whereis(Buffer) of
    undefined -> ok;
    Pid       -> gen:stop(Pid, normal, infinity)
  end.

-spec child_spec(Opts :: opts()) -> supervisor:child_spec().
child_spec(Opts) when is_list(Opts) ->
  child_spec(maps:from_list(Opts));

child_spec(Opts) when is_map(Opts) ->
  Buffer = maps:get(buffer, Opts),

  #{
    id    => Buffer,
    start => {?MODULE, start_link, [Buffer, Opts]},
    type  => supervisor
  }.

%% @equiv eval(Buffer, Message, 1)
eval(Buffer, Message) ->
  eval(Buffer, Message, 1).

-spec eval(
        Buffer  :: t(),
        Message :: any(),
        Retries :: non_neg_integer()
      ) -> any().
eval(Buffer, Message, Retries) when is_integer(Retries); Retries >= 0 ->
  case gen_buffer_lib:get_available_worker(Buffer) of
    no_available_workers ->
      maybe_retry(Buffer, Message, Retries - 1);

    Worker ->
      gen_buffer_worker:eval(Worker, Message)
  end.

%% @equiv send(Buffer, Message, self())
send(Buffer, Message) ->
  send(Buffer, Message, self()).

-spec send(
        Buffer  :: t(),
        Message :: any(),
        ReplyTo :: pid() | atom() | {atom(), node()}
      ) -> reference().
send(Buffer, Message, ReplyTo) ->
  Ref = make_ref(),
  WorkerMessage = {ReplyTo, Ref, Message},

  case gen_buffer_lib:get_available_worker(Buffer) of
    no_available_workers ->
      BufferPartition = gen_buffer_lib:get_partition(Buffer),
      _ = ets_buffer:write_dedicated(BufferPartition, WorkerMessage),
      Ref;

    Worker ->
      ok = gen_buffer_worker:send(Worker, WorkerMessage),
      Ref
  end.

-spec poll(Buffer :: t()) -> ok | {error, term()}.
poll(Buffer) when is_atom(Buffer) ->
  case get_worker(Buffer) of
    {ok, Worker} ->
      gen_buffer_worker:poll(Worker);

    Error ->
      Error
  end.

%% @equiv recv(Buffer, Ref, infinity)
recv(Buffer, Ref) ->
  recv(Buffer, Ref, infinity).

-spec recv(
        Buffer  :: t(),
        Ref     :: reference(),
        Timeout :: timeout()
      ) -> {ok, HandlerResponse :: any()} | {error, Reason :: any()}.
recv(Buffer, Ref, Timeout) ->
  receive
    {reply, Ref, Buffer, Payload} ->
      {ok, Payload};

    {error, Ref, Buffer, Error} ->
      {error, Error}
  after
    Timeout -> {error, timeout}
  end.

%% @equiv send_recv(Buffer, Message, infinity)
send_recv(Buffer, Message) ->
  send_recv(Buffer, Message, infinity).

-spec send_recv(
        Buffer  :: t(),
        Message :: any(),
        Timeout :: timeout()
      ) -> {ok, HandlerResponse :: any()} | {error, Reason :: any()}.
send_recv(Buffer, Message, Timeout) ->
  Ref = send(Buffer, Message),
  recv(Buffer, Ref, Timeout).

-spec get_worker(Buffer :: t()) -> {ok, pid()} | {error, term()}.
get_worker(Buffer) ->
  case get_children(Buffer) of
    {ok, Children} ->
      Nth = erlang:phash2(os:timestamp(), length(Children)) + 1,
      {_, Pid, _, _} = lists:nth(Nth, Children),
      {ok, Pid};

    Error ->
      Error
  end.

-spec get_workers(Buffer :: t()) -> {ok, [pid()]} | {error, term()}.
get_workers(Buffer) ->
  case get_children(Buffer) of
    {ok, Children} ->
      {ok, [Pid || {_, Pid, _, _} <- Children]};

    Error ->
      Error
  end.

%% @equiv incr_worker(Buffer, #{})
incr_worker(Buffer) ->
  incr_worker(Buffer, #{}).

-spec incr_worker(
        Buffer :: t(),
        Opts   :: opts()
      ) -> supervisor:startchild_ret().
incr_worker(Buffer, Opts) ->
  gen_buffer_sup:start_child(Buffer, Opts).

-spec decr_worker(Buffer :: t()) -> ok.
decr_worker(Buffer) ->
  gen_buffer_sup:terminate_child(Buffer).

%% @equiv set_workers(Buffer, N, #{})
set_workers(Buffer, N) ->
  set_workers(Buffer, N, #{}).

-spec set_workers(
        Buffer :: t(),
        N      :: non_neg_integer(),
        Opts   :: opts()
      ) -> {ok, [pid()]} | {error, term()}.
set_workers(Buffer, N, Opts) when N > 0 ->
  try
    NumWorkers =
      case get_workers(Buffer) of
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

    ok =
      lists:foreach(fun
        (_) when is_function(Fun, 2) -> Fun(Buffer, Opts);
        (_) when is_function(Fun, 1) -> Fun(Buffer)
      end, lists:seq(1, Diff)),

    get_workers(Buffer)
  catch
    throw:Ex -> Ex
  end.

-spec size(Buffer :: t()) -> integer() | undefined.
size(Buffer) ->
  case whereis(Buffer) of
    undefined ->
      undefined;

    _ ->
      % we have to subtract 1 because ets_buffer uses one entry for metadata
      Partitions = gen_buffer_lib:get_one_metadata_value(Buffer, partitions, []),

      lists:foldl(fun(Partition, Acc) ->
        Acc + (ets:info(Partition, size) - 1)
      end, 0, Partitions)
  end.

-spec info(Buffer :: t()) -> buffer_info() | undefined.
info(Buffer) ->
  buffer_info(Buffer).

-spec info() -> buffers_info().
info() ->
  lists:foldl(fun
    ({gen_buffer, Buffer}, Acc) ->
      case buffer_info(Buffer) of
        undefined -> Acc;
        InfoChann -> Acc#{Buffer => InfoChann}
      end;

    (_, Acc) ->
      Acc
  end, #{}, pg2:which_groups()).

%%%===================================================================
%%% Global Utilities
%%%===================================================================

-spec pg2_namespace(gen_buffer:t()) -> any().
pg2_namespace(Buffer) ->
  {gen_buffer, Buffer}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
get_children(undefined) ->
  {error, no_available_buffer};

get_children(Buffer) when is_atom(Buffer) ->
  get_children(whereis(Buffer));

get_children(Buffer) when is_pid(Buffer) ->
  case supervisor:which_children(Buffer) of
    []       -> {error, no_available_workers};
    Children -> {ok, Children}
  end.

%% @private
maybe_retry(_Buffer, _Message, 0) ->
  no_available_workers;

maybe_retry(Buffer, Message, Retries) when Retries < 0 ->
  eval(Buffer, Message, 0);

maybe_retry(Buffer, Message, Retries) ->
  eval(Buffer, Message, Retries - 1).

%% @private
buffer_info(Buffer) ->
  case get_workers(Buffer) of
    {ok, WorkerList} ->
      info_map(length(WorkerList), ?MODULE:size(Buffer));

    {error, no_available_workers} ->
      info_map(0, ?MODULE:size(Buffer));

    {error, no_available_buffer} ->
      undefined
  end.

%% @private
info_map(Workers, QueueSize) ->
  #{
    workers    => Workers,
    size => QueueSize
  }.
