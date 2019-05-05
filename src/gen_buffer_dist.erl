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
  sync_send_recv/2,
  sync_send_recv/3,
  get_worker/1,
  get_workers/1,
  set_workers/2,
  set_workers/3,
  set_workers/4,
  queue_size/1,
  queue_size/2,
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

%% @equiv gen_buffer:start_link(Channel, Opts)
start_link(Channel, Opts) ->
  gen_buffer:start_link(Channel, Opts).

%% @equiv gen_buffer:stop(Channel)
stop(Channel) ->
  gen_buffer:stop(Channel).

%% @equiv eval(Channel, Message, 1)
eval(Channel, Message) ->
  eval(Channel, Message, 1).

-spec eval(
        Channel :: gen_buffer:channel(),
        Message :: any(),
        Retries :: integer()
      ) -> any() | rpc_error().
eval(Channel, Message, Retries)->
  rpc_call(Channel, eval, [Channel, Message, Retries]).

-spec send(
        Channel :: gen_buffer:channel(),
        Message :: any()
      ) -> reference() | rpc_error().
send(Channel, Message) ->
  rpc_call(Channel, send, [Channel, Message, self()]).

-spec poll(
        Channel :: gen_buffer:channel()
      ) -> ok | {error, Reason :: any()} | rpc_error().
poll(Channel) ->
  rpc_call(Channel, poll, [Channel]).

%% @equiv recv(Channel, Ref, infinity)
recv(Channel, Ref) ->
  recv(Channel, Ref, infinity).

-spec recv(
        Channel :: gen_buffer:channel(),
        Ref     :: reference(),
        Timeout :: timeout()
      ) -> {ok, any()} | {error, any()} | {badrpc, any()}.
recv(Channel, Ref, Timeout) ->
  gen_buffer:recv(Channel, Ref, Timeout).

%% @equiv sync_send_recv(Channel, Message, infinity)
sync_send_recv(Channel, Message) ->
  sync_send_recv(Channel, Message, infinity).

-spec sync_send_recv(
        Channel :: gen_buffer:channel(),
        Message :: any(),
        Timeout :: timeout()
      ) -> {ok, any()} | {error, any()} | rpc_error().
sync_send_recv(Channel, Message, Timeout) ->
  rpc_call(Channel, sync_send_recv, [Channel, Message, Timeout]).

-spec get_worker(Channel :: gen_buffer:channel()) -> {ok, pid()} | {error, any()}.
get_worker(Channel) ->
  gen_buffer:get_worker(Channel).

-spec get_workers(Channel :: gen_buffer:channel()) -> [{node(), [pid()]}].
get_workers(Channel) ->
  Nodes = get_nodes(Channel),
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, get_workers, [Channel]),
  WorkersL = [Workers || {OkOrErr, Workers} <- ResL, OkOrErr =:= ok],
  lists:zip(Nodes, WorkersL).

%% @equiv set_workers(Channel, N, #{})
set_workers(Channel, N) ->
  set_workers(Channel, N, #{}).

%% @equiv set_workers(get_nodes(Channel), Channel, N, Opts)
set_workers(Channel, N, Opts) ->
  set_workers(get_nodes(Channel), Channel, N, Opts).

-spec set_workers(
        Nodes   :: [node()],
        Channel :: gen_buffer:channel(),
        N       :: non_neg_integer(),
        Opts    :: gen_buffer:opts()
      ) -> dist_res([pid()]).
set_workers(Nodes, Channel, N, Opts) when N > 0 ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, set_workers, [Channel, N, Opts]),
  WorkersL = [Workers || {OkOrErr, Workers} <- ResL, OkOrErr =:= ok],
  lists:zip(Nodes, WorkersL).

%% @equiv queue_size(get_nodes(Channel), Channel)
queue_size(Channel) ->
  queue_size(get_nodes(Channel), Channel).

-spec queue_size(
        Nodes   :: [node()],
        Channel :: gen_buffer:channel()
      ) -> dist_res(non_neg_integer()).
queue_size(Nodes, Channel) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, queue_size, [Channel]),
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
        ChannelOrNodes :: gen_buffer:channel() | [node()]
      ) -> dist_res(gen_buffer:channels_info()).
info(Channel) when is_atom(Channel) ->
  info(get_nodes(Channel), Channel);

info(Nodes) when is_list(Nodes) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, info, []),
  InfoL = [Info || Info <- ResL],
  lists:zip(Nodes, InfoL).

-spec info(
        Nodes   :: [node()],
        Channel :: gen_buffer:channel()
      ) -> dist_res(gen_buffer:channel_info()).
info(Nodes, Channel) ->
  {ResL, _} = rpc:multicall(Nodes, ?LOCAL, info, [Channel]),
  InfoL = [Info || Info <- ResL, Info =/= undefined],
  lists:zip(Nodes, InfoL).

%%%===================================================================
%%% Global Utilities
%%%===================================================================

%% @equiv pick_node(Channel, os:timestamp())
pick_node(Channel) ->
  pick_node(Channel, os:timestamp()).

-spec pick_node(Channel :: gen_buffer:channel(), Key :: any()) -> node().
pick_node(Channel, Key) ->
  Nodes = get_nodes(Channel),
  Nth = erlang:phash2(Key, length(Nodes)) + 1,
  lists:nth(Nth, Nodes).

-spec get_nodes(Channel :: gen_buffer:channel()) -> [node()].
get_nodes(Channel) ->
  Group = gen_buffer:pg2_namespace(Channel),

  case pg2:get_members(Group) of
    {error, {no_such_group, Group}} ->
      ok = pg2:create(Group),
      get_nodes(Channel);

    [] ->
      error(no_available_nodes);

    Pids ->
      [node(Pid) || Pid <- Pids]
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
rpc_call(Channel, Fun, Args) ->
  rpc_call(Channel, ?LOCAL, Fun, Args).

%% @private
rpc_call(Channel, Mod, Fun, Args) ->
  LocalNode = node(),
  case pick_node(Channel) of
    LocalNode  -> apply(Mod, Fun, Args);
    RemoteNode -> rpc:call(RemoteNode, Mod, Fun, Args)
  end.
