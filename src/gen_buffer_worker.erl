%%%-------------------------------------------------------------------
%%% @doc
%%% gen_buffer worker.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer_worker).

-behaviour(gen_server).

%% API
-export([
  start_link/1,
  start_link/2,
  eval/2,
  send/2,
  poll/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv start_link(Opts, #{})
start_link(Opts) when is_map(Opts) ->
  start_link(Opts, #{}).

-spec start_link(
        Opts    :: gen_buffer:opts_map(),
        ExtOpts :: gen_buffer:opts_map()
      ) -> {ok, pid()} | ignore | {error, term()}.
start_link(Opts, ExtOpts) when is_map(Opts), is_map(ExtOpts) ->
  gen_server:start_link(?MODULE, maps:merge(Opts, ExtOpts), []).

-spec eval(Worker :: pid(), Message :: any()) -> ok.
eval(Worker, Message) ->
  gen_server:call(Worker, {eval, Message}).

-spec send(Worker :: pid(), Message :: any()) -> ok.
send(Worker, Message) ->
  gen_server:cast(Worker, {send, Message}).

-spec poll(Worker :: pid()) -> ok.
poll(Worker) ->
  gen_server:cast(Worker, poll).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @hidden
init(#{buffer := Buffer} = Opts) ->
  _ = process_flag(trap_exit, true),
  ok = worker_available(Buffer, true),

  State = validate_opts(Opts),
  Handler = maps:get(message_handler, State),
  InitResult =
    case erlang:function_exported(Handler, init, 1) of
      true  -> Handler:init(maps:get(init_args, State, undefined));
      false -> {ok, undefined}
    end,

  handle_init_result(InitResult, State).

%% @hidden
handle_call({eval, Message}, _From, State) ->
  {ok, Res, NewHState} = do_tx(fun eval_callback/2, [Message, State], State),
  {reply, Res, State#{handler_state := NewHState}};

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%% @hidden
handle_cast({send, Message}, State) ->
  NewState = do_tx(fun do_poll/2, [Message, State], State),
  {noreply, NewState};

handle_cast(poll, State) ->
  {noreply, do_poll(undefined, State)}.

%% @hidden
handle_info(
      Info,
      #{
        buffer          := Buffer,
        message_handler := Handler,
        handler_state   := HState
      } = State
    ) ->
  case erlang:function_exported(Handler, handle_info, 3) of
    true ->
      try
        ok = worker_available(Buffer, false),
        Result = Handler:handle_info(Buffer, Info, HState),
        handle_info_result(Result, State)
      catch
        throw:Error -> handle_info_result(Error, State)
      after
        ok = worker_available(Buffer, true)
      end;

    false ->
      {noreply, State}
  end.

%% @hidden
terminate(Reason, #{buffer := Buffer, message_handler := Handler, handler_state := HState}) ->
  ok = worker_available(Buffer, false),
  case erlang:function_exported(Handler, terminate, 3) of
    true ->
      try
        Handler:terminate(Buffer, Reason, HState)
      catch
        throw:Response -> Response
      end;

    false ->
      ok
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
validate_opts(Opts) ->
  validate_handler(Opts).

%% @private
validate_handler(Opts) ->
  case maps:get(message_handler, Opts, nil) of
    nil ->
      exit({missing_option, message_handler});

    Handler when is_atom(Handler) ->
      validate_message_handler_mod(Handler, Opts);

    Handler ->
      exit({invalid_message_handler, Handler})
  end.

%% @private
validate_message_handler_mod(Handler, Opts) ->
  case code:ensure_loaded(Handler) of
    {module, Handler} ->
      validate_message_handler_funs(Handler, Opts);

    {error, _} ->
      exit({invalid_message_handler, Handler})
  end.

%% @private
validate_message_handler_funs(Handler, Opts) ->
  case erlang:function_exported(Handler, handle_message, 3) of
    true ->
      Opts;

    false ->
      exit({missing_callback, handle_message})
  end.

%% @priate
do_poll(Message, #{buffer := Buffer} = State) ->
  State1 =
    case Message of
      undefined ->
        State;

      _ ->
        {ok, _Res, NewHState} = eval_callback(Message, State),
        State#{handler_state := NewHState}
    end,

  lists:foldl(fun(Partition, Acc) ->
    PartitionName = gen_buffer_lib:partition_name(Buffer, Partition),
    do_partition_poll(PartitionName, Acc)
  end, State1, lists:seq(0, maps:get(n_partitions, State, 1) - 1)).

%% @priate
do_partition_poll(BufferPartition, State) ->
  case ets_buffer:read_dedicated(BufferPartition) of
    [] ->
      State;

    [Message] ->
      {ok, _Res, NewHState} = eval_callback(Message, State),
      do_partition_poll(BufferPartition, State#{handler_state := NewHState});

    {missing_ets_data, BufferPartition, _ReadLoc} ->
      % this case is expected under heavy concurrency scenario;
      % see ets_buffer for more details
      do_partition_poll(BufferPartition, State)
  end.

%% @priate
do_tx(Fun, Args, #{buffer := Buffer}) ->
  try
    ok = worker_available(Buffer, false),
    apply(Fun, Args)
  after
    ok = worker_available(Buffer, true)
  end.

-ifdef(OTP_RELEASE).
%% OTP 21 or higher

%% @private
eval_callback(
      {From, Ref, Msg},
      #{
        buffer          := Buffer,
        message_handler := Handler,
        handler_state   := HState
      } = State
    ) ->
  try Handler:handle_message(Buffer, Msg, HState) of
    {ok, Reply, _} = Res ->
      _ = maybe_reply({reply, Ref, Buffer, Reply}, From, State),
      Res
  catch
    Class:Exception:Stacktrace ->
      _ = maybe_reply({error, Ref, Buffer, Exception}, From, State),
      erlang:raise(Class, Exception, Stacktrace)
  end;

eval_callback(Msg, #{buffer := Buffer, message_handler := Handler, handler_state := HState}) ->
  Handler:handle_message(Buffer, Msg, HState).

-else.
%% OTP 20 or lower

%% @private
eval_callback(
      {From, Ref, Msg},
      #{
        buffer          := Buffer,
        message_handler := Handler,
        handler_state   := HState
      } = State
    ) ->
  try Handler:handle_message(Buffer, Msg, HState) of
    {ok, Reply, _} = Res ->
      _ = maybe_reply({reply, Ref, Buffer, Reply}, From, State),
      Res
  catch
    Class:Exception ->
      _ = maybe_reply({error, Ref, Buffer, Exception}, From, State),
      erlang:raise(Class, Exception, erlang:get_stacktrace())
  end;

eval_callback(Msg, #{buffer := Buffer, message_handler := Handler, handler_state := HState}) ->
  Handler:handle_message(Buffer, Msg, HState).

-endif.

%% @private
maybe_reply(Reply, From, #{send_replies := true}) -> From ! Reply;
maybe_reply(_, _, _)                              -> ok.

%% @private
handle_init_result({ok, HState}, State) ->
  {ok, State#{handler_state => HState}};

handle_init_result({ok, HState, hibernate}, State) ->
  {ok, State#{handler_state => HState}, hibernate};

handle_init_result({ok, HState, Timeout}, State) ->
  {ok, State#{handler_state => HState}, Timeout};

handle_init_result(ignore, _State) ->
  ignore;

handle_init_result({stop, Reason}, #{buffer := Buffer} = State) ->
  ok = worker_available(Buffer, false),
  {stop, Reason, State}.

%% @private
handle_info_result({noreply, NewHState}, State) ->
  {noreply, State#{handler_state => NewHState}};

handle_info_result({noreply, NewHState, hibernate}, State) ->
  {noreply, State#{handler_state => NewHState}, hibernate};

handle_info_result({noreply, NewHState, Timeout}, State) ->
  {noreply, State#{handler_state => NewHState}, Timeout};

handle_info_result({stop, Reason, NewHState}, State) ->
  {stop, Reason, State#{handler_state => NewHState}}.

%% @private
worker_available(Buffer, true) ->
  gen_buffer_lib:set_metadata_value(Buffer, Buffer, self());

worker_available(Buffer, false) ->
  gen_buffer_lib:del_metadata_value(Buffer, Buffer, self()).
