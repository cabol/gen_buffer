-module(gen_buffer_SUITE).

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
  {gen_buffer_test_cases, [
    t_eval/1,
    t_eval_error/1,
    t_send_and_recv/1,
    t_send_and_recv_errors/1,
    t_send_recv/1,
    t_fire_and_forget/1,
    t_add_del_workers/1,
    t_set_workers/1,
    t_size/1,
    t_info_buffer/1,
    t_info/1,
    t_worker_polling/1,
    t_worker_distribution/1
  ]}
]).

%% Test Cases
-export([
  t_child_spec/1,
  t_create_errors/1,
  t_delete_buffer/1,
  t_buffer_types/1,
  t_missing_buffer_worker_funs/1,
  t_handle_message_state/1,
  t_callback_init/1,
  t_callback_handle_info/1,
  t_callback_terminate/1,
  t_restart_workers/1,
  t_gen_buffer_dist_locally/1,
  t_gen_buffer_worker_missing_ets_data/1,
  t_gen_buffer_lib_missing_funs/1
]).

%% Helpers
-export([
  producer/2
]).

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite,
  init_per_testcase,
  end_per_testcase,
  producer
]).

-define(BUFFER, gen_buffer_test).

%%%===================================================================
%%% Common Test
%%%===================================================================

all() ->
  Exports = ?MODULE:module_info(exports),
  [F || {F, _} <- Exports, not lists:member(F, ?EXCLUDED_FUNS)].

init_per_suite(Config) ->
  Opts = #{
    message_handler => test_message_handler,
    send_replies    => true,
    init_args       => ok
  },

  [{opts, Opts}, {module, gen_buffer} | Config].

end_per_suite(Config) ->
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  Mod = ?config(module, Config),
  ok = cleanup_buffers(Mod),
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_child_spec(_Config) ->
  #{
    id := test_buffer,
    start := {
      gen_buffer,
      start_link,
      [
        test_buffer,
        #{
          buffer := test_buffer,
          message_handler := ?MODULE
        }
      ]
    },
    type := supervisor
  } = gen_buffer:child_spec([{buffer, test_buffer}, {message_handler, ?MODULE}]).

t_create_errors(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),

  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  {error, {already_started, _}} =
    gen_buffer_ct:create_buffer(?BUFFER, [{message_handler, handler}], Mod, Config),

  _ = process_flag(trap_exit, true),

  {{invalid_message_handler, handler}, _} =
    try
      gen_buffer_ct:create_buffer(test, [{message_handler, handler}], Mod, Config)
    catch
      _:E2 -> E2
    end,

  {{missing_option, message_handler}, _} =
    try
      gen_buffer_ct:create_buffer(test, [], Mod, Config)
    catch
      _:E3 -> E3
    end,

  {{missing_callback, handle_message}, _} =
    try
      gen_buffer_ct:create_buffer(
        test,
        Opts#{message_handler => test_message_handler3},
        Mod,
        Config
      )
    catch
      _:E4 -> E4
    end,

  {{invalid_message_handler,"wrong_handler"}, _} =
    try
      gen_buffer_ct:create_buffer(test, Opts#{message_handler => "wrong_handler"}, Mod, Config)
    catch
      _:E5 -> E5
    end,

  {ok, _} =
    gen_buffer_ct:create_buffer(
      test1,
      Opts#{message_handler => test_message_handler2},
      Mod,
      Config
    ),

  {ok, _} = gen_buffer_ct:create_buffer(test, Opts, Mod, Config).

t_delete_buffer(Config) ->
  Mod = ?config(module, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, ?config(opts, Config), Mod, Config),

  true = is_pid(whereis(?BUFFER)),
  ok = Mod:stop(?BUFFER),
  ok = Mod:stop(wrong_buffer),
  undefined = whereis(?BUFFER).

t_buffer_types(Config) ->
  Mod = ?config(module, Config),

  Opts = (?config(opts, Config))#{
    send_replies => false,
    workers      => 0
  },

  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),
  _ = gen_buffer_ct:create_buffer(ring, Opts#{buffer_type => ring, buffer_size => 5}, Mod, Config),
  _ = gen_buffer_ct:create_buffer(lifo, Opts#{buffer_type => lifo}, Mod, Config),
  ChannName = gen_buffer_lib:partition_name(?BUFFER, 0),

  lists:foreach(fun(M) ->
    Mod:send(?BUFFER, M)
  end, lists:seq(1, 10)),

  [{_, _, 1}] = ets_buffer:read_dedicated(ChannName),

  lists:foreach(fun(M) ->
    Mod:send(lifo, M)
  end, lists:seq(1, 10)),

  [{_, _, 10}] = ets_buffer:read_dedicated('lifo.0'),

  lists:foreach(fun(M) ->
    Mod:send(ring, M)
  end, lists:seq(1, 10)),

  [{_, _, 6}] = ets_buffer:read_dedicated('ring.0').

t_missing_buffer_worker_funs(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),

  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),
  _ = gen_buffer_worker:start_link(Opts#{buffer => ?BUFFER}),

  {ok, Worker} = Mod:get_worker(?BUFFER),
  ok = gen_server:call(Worker, ping).

t_handle_message_state(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts#{workers => 1}, Mod, Config),

  ok = lists:foreach(fun(M) ->
    {ok, M} = Mod:send_recv(?BUFFER, M)
  end, lists:seq(1, 10)),

  11 = Mod:eval(?BUFFER, 11),

  _ = timer:sleep(1000),
  {ok, MsgL} = Mod:send_recv(?BUFFER, messages),
  11 = length(MsgL).

t_callback_init(Config) ->
  Mod = ?config(module, Config),
  Opts = (?config(opts, Config))#{workers => 1},

  _ = process_flag(trap_exit, true),
  Opts2 = Opts#{init_args => {test, {stop, kill}}},

  try gen_buffer_ct:create_buffer(?BUFFER, Opts2, Mod, Config)
  catch
    exit:kill -> ok
  end,

  OptsL = [
    Opts#{init_args => {test, {ok, #{}}}},
    Opts#{init_args => {test, {ok, #{}, hibernate}}},
    Opts#{init_args => {test, {ok, #{}, 5000}}},
    Opts#{init_args => {test, ignore}}
  ],

  lists:foreach(fun(Args) ->
    _ = gen_buffer_ct:create_buffer(?BUFFER, Args, Mod, Config),
    _ = timer:sleep(1000),
    Mod:stop(?BUFFER)
  end, OptsL).

t_callback_handle_info(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  Messages = [
    {noreply, #{}},
    {noreply, #{}, hibernate},
    {noreply, #{}, 1},
    {stop, kill, #{}},
    throw,
    terminate
  ],

  ok = lists:foreach(fun(Info) ->
    {ok, Worker} = gen_buffer:get_worker(?BUFFER),
    Worker ! Info
  end, Messages),

  Opts1 = Opts#{message_handler => test_message_handler2},
  _ = gen_buffer_ct:create_buffer(test, Opts1, Mod, Config),

  {ok, Worker} = gen_buffer:get_worker(test),
  Worker ! {stop, kill, #{}},

  Opts2 = Opts#{message_handler => test_message_handler5},
  _ = gen_buffer_ct:create_buffer(test2, Opts2, Mod, Config),

  {ok, Worker2} = gen_buffer:get_worker(test2),
  Worker2 ! {stop, kill, #{}},

  Opts3 = Opts#{message_handler => test_message_handler4},
  _ = gen_buffer_ct:create_buffer(test3, Opts3, Mod, Config),

  {ok, Worker3} = gen_buffer:get_worker(test3),
  Worker3 ! "hello",
  _ = timer:sleep(500),
  AvailableWorkers = gen_buffer_lib:get_available_workers(test3),
  false = lists:member(Worker3, AvailableWorkers).

t_callback_terminate(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  {ok, Worker1} = gen_buffer:get_worker(?BUFFER),
  _ = exit(Worker1, shutdown),

  {ok, Worker2} = gen_buffer:get_worker(?BUFFER),
  _ = exit(Worker2, throw),

  Opts1 = Opts#{message_handler => test_message_handler2},
  _ = gen_buffer_ct:create_buffer(test, Opts1, Mod, Config),

  {ok, Worker3} = gen_buffer:get_worker(test),
  _ = exit(Worker3, shutdown).

t_restart_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  {ok, WorkerL1} = gen_buffer:get_workers(?BUFFER),
  Len1 = length(WorkerL1),

  {ok, Worker1} = gen_buffer:get_worker(?BUFFER),
  _ = exit(Worker1, shutdown),

  {ok, Worker2} = gen_buffer:get_worker(?BUFFER),
  _ = exit(Worker2, shutdown),

  {ok, WorkerL2} = gen_buffer:get_workers(?BUFFER),
  Len1 = length(WorkerL2).

t_gen_buffer_dist_locally(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  {ok, "hello"} = gen_buffer_dist:send_recv(?BUFFER, "hello").

t_gen_buffer_worker_missing_ets_data(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  _ = process_flag(trap_exit, true),
  Pids = [spawn_link(?MODULE, producer, [Mod, ?BUFFER]) || _ <- lists:seq(1, 20)],
  _ = timer:sleep(10000),
  ok = lists:foreach(fun(Pid) -> exit(Pid, normal) end, Pids),
  Mod:stop(?BUFFER).

t_gen_buffer_lib_missing_funs(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  undefined = gen_buffer_lib:get_metadata_value(?BUFFER, hello),
  hello = gen_buffer_lib:get_one_metadata_value(?BUFFER, hello, hello).

%%%===================================================================
%%% Helpers
%%%===================================================================

producer(Mod, Buffer) ->
  _ = Mod:send(Buffer, "hello"),
  ok = Mod:poll(Buffer),
  producer(Mod, Buffer).

%%%===================================================================
%%% Internal functions
%%%===================================================================

cleanup_buffers(Mod) ->
  lists:foreach(fun(Ch) ->
    Mod:stop(Ch)
  end, [?BUFFER, test, test2, test3]).
