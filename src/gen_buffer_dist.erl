%%%-------------------------------------------------------------------
%%% @doc
%%% gen_buffer Distributed Interface.
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer_dist).

%% API
-export([
  start_link/2,
  stop/1,
  eval/2,
  eval/3,
  send/2,
  poll/1,
  recv/2,
  recv/3,
  send_recv/2,
  send_recv/3,
  get_worker/1,
  get_workers/1,
  set_workers/2,
  set_workers/3,
  set_workers/4,
  size/1,
  size/2,
  info/0,
  info/1,
  info/2
]).

%% Global Utilities
-export([
  pick_node/1,
  pick_node/2,
  get_nodes/1
]).

-define(LOCAL, gen_buffer).

%%%===================================================================
%%% Types
%%%===================================================================

-type dist_res(R) :: [{Node ::node(), R}].
-type rpc_error() :: {badrpc, Reason :: any()} | no_return().

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv gen_buffer:start_link(Buffer, Opts)
start_link(Buffer, Opts) ->
  gen_buffer:start_link(Buffer, Opts).

%% @equiv gen_buffer:stop(Buffer)
stop(Buffer) ->
  gen_buffer:stop(Buffer).

%% @equiv eval(Buffer, Message, 1)
eval(Buffer, Message) ->
  eval(Buffer, Message, 1).

-spec eval(
        Buffer :: gen_buffer:t(),
        Message :: any(),
        Retries :: integer()
      ) -> any() | rpc_error().
eval(Buffer, Message, Retries)->
  rpc_call(Buffer, eval, [Buffer, Message, Retries]).

-spec send(
        Buffer :: gen_buffer:t(),
        Message :: any()
      ) -> reference() | rpc_error().
send(Buffer, Message) ->
  rpc_call(Buffer, send, [Buffer, Message, self()]).

-spec poll(
        Buffer :: gen_buffer:t()
      ) -> ok | {error, Reason :: any()} | rpc_error().
poll(Buffer) ->
  rpc_call(Buffer, poll, [Buffer]).

%% @equiv recv(Buffer, Ref, infinity)
recv(Buffer, Ref) ->
  recv(Buffer, Ref, infinity).

-spec recv(
        Buffer :: gen_buffer:t(),
        Ref     :: reference(),
        Timeout :: timeout()
      ) -> {ok, any()} | {error, any()} | {badrpc, any()}.
recv(Buffer, Ref, Timeout) ->
  gen_buffer:recv(Buffer, Ref, Timeout).

%% @equiv send_recv(Buffer, Message, infinity)
send_recv(Buffer, Message) ->
  send_recv(Buffer, Message, infinity).

-spec send_recv(
        Buffer :: gen_buffer:t(),
        Message :: any(),
        Timeout :: timeout()
      ) -> {ok, any()} | {error, any()} | rpc_error().
send_recv(Buffer, Message, Timeout) ->
  rpc_call(Buffer, send_recv, [Buffer, Message, Timeout]).

-spec get_worker(Buffer :: gen_buffer:t()) -> {ok, pid()} | {error, any()}.
get_worker(Buffer) ->
  gen_buffer:get_worker(Buffer).

-spec get_workers(Buffer :: gen_buffer:t()) -> [{node(), [pid()]}].
get_workers(Buffer) ->
  Nodes = get_nodes(Buffer),
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, get_workers, [Buffer]),
  WorkersL = [Workers || {OkOrErr, Workers} <- ResL, OkOrErr =:= ok],
  lists:zip(Nodes, WorkersL).

%% @equiv set_workers(Buffer, N, #{})
set_workers(Buffer, N) ->
  set_workers(Buffer, N, #{}).

%% @equiv set_workers(get_nodes(Buffer), Buffer, N, Opts)
set_workers(Buffer, N, Opts) ->
  set_workers(get_nodes(Buffer), Buffer, N, Opts).

-spec set_workers(
        Nodes   :: [node()],
        Buffer :: gen_buffer:t(),
        N       :: non_neg_integer(),
        Opts    :: gen_buffer:opts()
      ) -> dist_res([pid()]).
set_workers(Nodes, Buffer, N, Opts) when N > 0 ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, set_workers, [Buffer, N, Opts]),
  WorkersL = [Workers || {OkOrErr, Workers} <- ResL, OkOrErr =:= ok],
  lists:zip(Nodes, WorkersL).

%% @equiv size(get_nodes(Buffer), Buffer)
size(Buffer) ->
  size(get_nodes(Buffer), Buffer).

-spec size(
        Nodes   :: [node()],
        Buffer :: gen_buffer:t()
      ) -> dist_res(non_neg_integer()).
size(Nodes, Buffer) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, size, [Buffer]),
  SizeL = [Size || Size <- ResL, Size =/= undefined],
  lists:zip(Nodes, SizeL).

%% equiv info(Nodes)
info() ->
  Nodes =
    lists:foldl(fun
      ({gen_buffer, _} = Group, Acc) ->
        GroupNodes = [node(Member) || Member <- pg2:get_members(Group)],
        GroupNodes ++ Acc;

      (_, Acc) ->
        Acc
    end, [], pg2:which_groups()),

  info(Nodes).

-spec info(
        BufferOrNodes :: gen_buffer:t() | [node()]
      ) -> dist_res(gen_buffer:buffers_info()).
info(Buffer) when is_atom(Buffer) ->
  info(get_nodes(Buffer), Buffer);

info(Nodes) when is_list(Nodes) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, info, []),
  InfoL = [Info || Info <- ResL],
  lists:zip(Nodes, InfoL).

-spec info(
        Nodes   :: [node()],
        Buffer :: gen_buffer:t()
      ) -> dist_res(gen_buffer:buffer_info()).
info(Nodes, Buffer) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, info, [Buffer]),
  InfoL = [Info || Info <- ResL, Info =/= undefined],
  lists:zip(Nodes, InfoL).

%%%===================================================================
%%% Global Utilities
%%%===================================================================

%% @equiv pick_node(Buffer, os:timestamp())
pick_node(Buffer) ->
  pick_node(Buffer, os:timestamp()).

-spec pick_node(Buffer :: gen_buffer:t(), Key :: any()) -> node().
pick_node(Buffer, Key) ->
  Nodes = get_nodes(Buffer),
  Nth = erlang:phash2(Key, length(Nodes)) + 1,
  lists:nth(Nth, Nodes).

-spec get_nodes(Buffer :: gen_buffer:t()) -> [node()].
get_nodes(Buffer) ->
  Group = gen_buffer:pg2_namespace(Buffer),

  case pg2:get_members(Group) of
    {error, {no_such_group, Group}} ->
      ok = pg2:create(Group),
      get_nodes(Buffer);

    [] ->
      error(no_available_nodes);

    Pids ->
      [node(Pid) || Pid <- Pids]
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
rpc_call(Buffer, Fun, Args) ->
  rpc_call(Buffer, ?LOCAL, Fun, Args).

%% @private
rpc_call(Buffer, Mod, Fun, Args) ->
  LocalNode = node(),
  case pick_node(Buffer) of
    LocalNode  -> apply(Mod, Fun, Args);
    RemoteNode -> rpc:call(RemoteNode, Mod, Fun, Args)
  end.
