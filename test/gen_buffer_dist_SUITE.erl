-module(gen_buffer_dist_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Common Test Cases
-include_lib("mixer/include/mixer.hrl").
-mixin([
  {gen_buffer_common_test_cases, [
    t_eval/1,
    t_eval_error/1,
    t_send_recv_error/1,
    t_sync_send_recv/1,
    t_fire_and_forget/1
  ]}
]).

%% Test Cases
-export([
  t_send_recv/1,
  t_get_set_workers/1,
  t_queue_size/1,
  t_info_channel/1,
  t_info/1,
  t_no_available_nodes/1
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite,
  init_per_testcase,
  end_per_testcase
]).

-define(helpers, gen_buffer_common_test_cases).
-define(CHANNEL, gen_buffer_test).
-define(SLAVES, ['node1@127.0.0.1', 'node2@127.0.0.1']).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

init_per_suite(Config) ->
  ok = start_primary_node(),
  {ok, _} = application:ensure_all_started(gen_buffer),
  ok = allow_boot(),
  Nodes = start_slaves(?SLAVES),

  Opts = #{
    message_handler => test_message_handler,
    send_replies    => true,
    init_args       => ok
  },

  [{nodes, Nodes}, {opts, Opts}, {module, gen_buffer_dist} | Config].

end_per_suite(Config) ->
  stop_slaves(?SLAVES),
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  ok = cleanup_remote_channels(),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_send_recv(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),

  Ref1 = Mod:send(?CHANNEL, "hello"),
  {reply, Ref1, ?CHANNEL, "hello"} = gen_buffer_ct:wait_for_msg(),

  Ref2 = Mod:send(?CHANNEL, "hello"),
  {ok, "hello"} = Mod:recv(?CHANNEL, Ref2),

  ok = Mod:poll(?CHANNEL),
  {error, timeout} = gen_buffer_ct:wait_for_msg(200),

  ok = Mod:stop(?CHANNEL),
  ok = Mod:stop(test).

t_get_set_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),

  [
    {'ct@127.0.0.1', Workers1_N1},
    {'node1@127.0.0.1', Workers1_N2},
    {'node2@127.0.0.1', Workers1_N3}
  ] = lists:usort(Mod:get_workers(?CHANNEL)),

  Len1 = erlang:system_info(schedulers_online),
  ok = lists:foreach(fun(WL) ->
    Len1 = length(WL)
  end, [Workers1_N1, Workers1_N2, Workers1_N3]),

  [
    {'ct@127.0.0.1', Workers2_N1},
    {'node1@127.0.0.1', Workers2_N2},
    {'node2@127.0.0.1', Workers2_N3}
  ] = lists:usort(Mod:set_workers(?CHANNEL, 3)),

  ok = lists:foreach(fun(WL) ->
    3 = length(WL)
  end, [Workers2_N1, Workers2_N2, Workers2_N3]),

  {ok, _} = Mod:get_worker(?CHANNEL).

t_queue_size(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),

  [
    {'ct@127.0.0.1', Size},
    {'node1@127.0.0.1', _},
    {'node2@127.0.0.1', _}
  ] = lists:usort(Mod:queue_size(?CHANNEL)),

  true = is_integer(Size).

t_info_channel(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),

  [
    {'ct@127.0.0.1', Data},
    {'node1@127.0.0.1', _},
    {'node2@127.0.0.1', _}
  ] = lists:usort(Mod:info(?CHANNEL)),

  #{workers := _, queue_size := _} = Data.

t_info(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),
  _ = ?helpers:create_channel(test, Opts, Mod, Config),

  ok = pg2:create(yet_another_group),

  [
    {'ct@127.0.0.1', Data},
    {'node1@127.0.0.1', _},
    {'node2@127.0.0.1', _}
  ] = lists:usort(Mod:info()),

  #{
    ?CHANNEL := #{workers := _, queue_size := _},
    test     := #{workers := _, queue_size := _}
  } = Data.

t_no_available_nodes(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),

  ok = pg2:delete(gen_buffer:pg2_namespace(?CHANNEL)),
  try
    Mod:send(?CHANNEL, "hello")
  catch
    error:no_available_nodes -> ok
  end,

  _ = ?helpers:create_channel(?CHANNEL, Opts, Mod, Config),
  _ = Mod:send(?CHANNEL, "hello").

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
start_primary_node() ->
  {ok, _} = net_kernel:start(['ct@127.0.0.1']),
  true = erlang:set_cookie(node(), gen_buffer),
  ok.

%% @private
allow_boot() ->
  _ = erl_boot_server:start([]),
  {ok, IPv4} = inet:parse_ipv4_address("127.0.0.1"),
  erl_boot_server:add_slave(IPv4).

%% @private
start_slaves(Slaves) ->
  start_slaves(Slaves, []).

%% @private
start_slaves([], Acc) ->
  lists:usort(Acc);

start_slaves([Node | T], Acc) ->
  start_slaves(T, [spawn_node(Node) | Acc]).

%% @private
spawn_node(Node) ->
  Cookie = atom_to_list(erlang:get_cookie()),
  InetLoaderArgs = "-loader inet -hosts 127.0.0.1 -setcookie " ++ Cookie,

  {ok, Node} =
    slave:start(
      "127.0.0.1",
      node_name(Node),
      InetLoaderArgs
    ),

  ok = rpc:block_call(Node, code, add_paths, [code:get_path()]),
  {ok, _} = rpc:block_call(Node, application, ensure_all_started, [gen_buffer]),
  ok = load_support_files(Node),
  Node.

%% @private
node_name(Node) ->
  [Name, _] = binary:split(atom_to_binary(Node, utf8), <<"@">>),
  binary_to_atom(Name, utf8).

%% @private
load_support_files(Node) ->
  {module, gen_buffer_common_test_cases} =
    rpc:block_call(Node, code, load_file, [gen_buffer_common_test_cases]),
  ok.

%% @private
stop_slaves(Slaves) ->
  stop_slaves(Slaves, []).

%% @private
stop_slaves([], Acc) ->
  lists:usort(Acc);
stop_slaves([Node | T], Acc) ->
  ok = slave:stop(Node),
  pang = net_adm:ping(Node),
  stop_slaves(T, [Node | Acc]).

cleanup_remote_channels() ->
  _ = register(ct, self()),
  Channels = [parent_gen_buffer_test, parent_test, parent_test2],
  [begin
    {Name, Node} ! exit,
    gen_buffer_ct:wait_for_msg(300)
  end || Name <- Channels, Node <- ?SLAVES],
  ok.
