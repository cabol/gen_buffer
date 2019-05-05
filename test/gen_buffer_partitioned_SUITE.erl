-module(gen_buffer_partitioned_SUITE).

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
    t_send_recv/1,
    t_send_recv_error/1,
    t_sync_send_recv/1,
    t_fire_and_forget/1,
    t_add_del_workers/1,
    t_set_workers/1,
    t_queue_size/1,
    t_info_channel/1,
    t_info/1,
    t_worker_polling/1,
    t_worker_distribution/1
  ]}
]).

-ifndef('CI').

%% Test Cases
-export([
  t_load_balancing/1
]).

-endif.

-define(EXCLUDED_FUNS, [
  module_info,
  all,
  init_per_suite,
  end_per_suite,
  init_per_testcase,
  end_per_testcase,
  producer
]).

-define(helpers, gen_buffer_common_test_cases).
-define(CHANNEL, gen_buffer_test).

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
    init_args       => ok,
    n_partitions    => erlang:system_info(schedulers_online)
  },

  [{opts, Opts}, {module, gen_buffer} | Config].

end_per_suite(Config) ->
  Config.

init_per_testcase(_, Config) ->
  Config.

end_per_testcase(_, Config) ->
  Config.

%%%===================================================================
%%% Tests Cases
%%%===================================================================

-ifndef('CI').

t_load_balancing(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = ?helpers:create_channel(?CHANNEL, Opts#{workers => 0}, Mod, Config),

  ok = lists:foreach(fun(M) ->
    Mod:send(?CHANNEL, {self(), M})
  end, lists:seq(1, 100)),

  ok = lists:foreach(fun(P) ->
    true = ets:info(gen_buffer_lib:partition_name(?CHANNEL, P), size) > 5
  end, lists:seq(0, 3)),

  {ok, [_, _, _, _]} = Mod:set_workers(?CHANNEL, 4),
  ok = Mod:poll(?CHANNEL),
  _ = timer:sleep(2000),

  lists:foreach(fun(P) ->
    1 = ets:info(gen_buffer_lib:partition_name(?CHANNEL, P), size)
  end, lists:seq(0, 3)).

-endif.
