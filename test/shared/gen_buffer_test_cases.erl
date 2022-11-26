-module(gen_buffer_test_cases).

-include_lib("common_test/include/ct.hrl").

%% Test Cases
-export([
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
]).

-define(BUFFER, gen_buffer_test).

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_eval(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  "hello" = Mod:eval(?BUFFER, "hello"),
  1 = Mod:eval(?BUFFER, 1),
  {ok, "hello"} = Mod:eval(?BUFFER, {ok, "hello"}),

  try Mod:eval(?BUFFER, error)
  catch
    exit:{{handler_exception, _}, _} -> ok
  end.

t_eval_error(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts#{workers => 0}, Mod, Config),

  no_available_workers = Mod:eval(?BUFFER, "hello", 5),

  true = register(ct, self()),
  _ = process_flag(trap_exit, true),

  Pid =
    spawn_link(fun() ->
      Res = Mod:eval(?BUFFER, "hello", 0),
      ct ! Res
    end),

  {error, timeout} = gen_buffer_ct:wait_for_msg(1000),
  true = unregister(ct),
  true = exit(Pid, normal).

t_send_and_recv(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  Ref1 = Mod:send(?BUFFER, "hello"),
  {reply, Ref1, ?BUFFER, "hello"} = gen_buffer_ct:wait_for_msg(),

  Ref2 = Mod:send(?BUFFER, "hello"),
  {ok, "hello"} = Mod:recv(?BUFFER, Ref2).

t_send_and_recv_errors(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  _ = Mod:send(?BUFFER, error),
  {error, _, ?BUFFER, handler_exception} = gen_buffer_ct:wait_for_msg(1000),

  {ok, _} = gen_buffer_ct:create_buffer(test, Opts#{send_replies => false}, Mod, Config),

  Ref = Mod:send(test, error),
  {error, timeout} = Mod:recv(test, Ref, 1000).

t_send_recv(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  {ok, "hello"} = Mod:send_recv(?BUFFER, "hello"),

  Opts1 = Opts#{message_handler => test_message_handler4},
  {ok, _} = gen_buffer_ct:create_buffer(test, Opts1, Mod, Config),

  {error, handler_exception} = Mod:send_recv(test, "hello"),

  Opts2 = Opts#{message_handler => test_message_handler4, send_replies => false},
  {ok, _} = gen_buffer_ct:create_buffer(test2, Opts2, Mod, Config),

  {error, timeout} = Mod:send_recv(test2, "hello", 1000).

t_fire_and_forget(Config) ->
  Mod = ?config(module, Config),
  Opts = (?config(opts, Config))#{message_handler => test_message_handler4, send_replies => false},
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  Ref = Mod:send(?BUFFER, "hello"),
  {error, timeout} = Mod:recv(?BUFFER, Ref, 500),

  {ok, _} =
    gen_buffer_ct:create_buffer(
      test, Opts#{message_handler => test_message_handler4}, Mod, Config),

  Ref2 = Mod:send(test, {self(), "hello"}),
  {error, timeout} = Mod:recv(?BUFFER, Ref2, 500).

t_add_del_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = maps:to_list(?config(opts, Config)),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),
  Workers = num_workers(Mod, ?BUFFER),

  ok = lists:foreach(fun(_) ->
    Mod:incr_worker(?BUFFER)
  end, lists:seq(1, 2)),

  ok = lists:foreach(fun(_) ->
    Mod:incr_worker(?BUFFER, Opts)
  end, lists:seq(3, 5)),

  Workers1 = num_workers(Mod, ?BUFFER),
  Workers1 = Workers + 5,

  ok = Mod:decr_worker(?BUFFER),
  ok = Mod:decr_worker(wrong_buffer),

  Workers2 = num_workers(Mod, ?BUFFER),
  Workers2 = Workers + 4,

  ok = lists:foreach(fun(_) ->
    Mod:decr_worker(?BUFFER)
  end, lists:seq(1, Workers2)),

  {error, no_available_buffer} = Mod:get_worker(wrong_buffer),
  {error, no_available_buffer} = Mod:get_workers(wrong_buffer),
  {error, no_available_buffer} = Mod:set_workers(wrong_buffer, 10, Opts),
  {error, no_available_workers} = Mod:decr_worker(?BUFFER),
  {error, no_available_workers} = Mod:poll(?BUFFER).

t_set_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = maps:to_list(?config(opts, Config)),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),
  Workers = num_workers(Mod, ?BUFFER),

  {ok, WorkersL1} = Mod:set_workers(?BUFFER, Workers + 2),
  Workers1 = length(WorkersL1),
  Workers1 = Workers + 2,

  {ok, WorkersL2} = Mod:set_workers(?BUFFER, Workers1 - 3, Opts),
  Workers2 = length(WorkersL2),
  Workers2 = Workers - 1,

  try
    Mod:set_workers(?BUFFER, -1, Opts)
  catch
    error:function_clause -> ok
  end.

t_size(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts#{workers => 0}, Mod, Config),

  0 = Mod:size(?BUFFER),

  ok = lists:foreach(fun(_) ->
    Mod:send(?BUFFER, "hello")
  end, lists:seq(1, 10)),

  10 = Mod:size(?BUFFER),
  undefined = Mod:size(undefined).

t_info_buffer(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),

  #{workers := W, size := QS} = Mod:info(?BUFFER),
  true = is_integer(W),
  true = is_integer(QS),
  undefined = Mod:info(undefined).

t_info(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts, Mod, Config),
  {ok, _} = gen_buffer_ct:create_buffer(test, Opts, Mod, Config),
  {ok, _} = gen_buffer_ct:create_buffer(test2, Opts#{workers => 0}, Mod, Config),
  ok = gen_buffer_pg:create(yet_another_group),

  InfoMap = Mod:info(),
  3 = map_size(InfoMap),

  #{
    ?BUFFER := #{workers := W, size := QS},
    test     := #{workers := _, size := _},
    test2    := #{workers := 0, size := _}
  } = InfoMap,

  true = is_integer(W),
  true = is_integer(QS).

t_worker_polling(Config) ->
  true = register(ct, self()),
  Mod = ?config(module, Config),

  Opts = ?config(opts, Config),

  Opts1 = Opts#{
    workers         => 0,
    send_replies    => false,
    message_handler => test_message_handler6
  },

  {ok, _} = gen_buffer_ct:create_buffer(?BUFFER, Opts1, Mod, Config),

  0 = Mod:size(?BUFFER),

  ok = lists:foreach(fun(_) ->
    Mod:send(?BUFFER, "hello")
  end, lists:seq(1, 5)),

  5 = Mod:size(?BUFFER),

  _ = Mod:set_workers(?BUFFER, 1),
  _ = timer:sleep(500),
  _ = Mod:poll(?BUFFER),

  ok = lists:foreach(fun(_) ->
    "hello" = gen_buffer_ct:wait_for_msg(1000)
  end, lists:seq(1, 5)),

  0 = Mod:size(?BUFFER),
  true = unregister(ct).

t_worker_distribution(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),

  {ok, _} =
    gen_buffer_ct:create_buffer(
      ?BUFFER, Opts#{message_handler => test_message_handler5}, Mod, Config),

  {ok, CurrentWorkers} = gen_buffer:get_workers(?BUFFER),
  NumMsgs = length(CurrentWorkers) * 100,
  Refs = [Mod:send(?BUFFER, M) || M <- lists:seq(1, NumMsgs)],
  no_available_workers = gen_buffer_lib:get_available_worker(?BUFFER),

  Replies =
    lists:foldl(fun(_, Acc) ->
      receive
        {reply, Ref, ?BUFFER, {_, Worker}} ->
          case lists:member(Ref, Refs) of
            true ->
              Count = maps:get(Worker, Acc, 0),
              Acc#{Worker => Count + 1};

            false ->
              Acc
          end;

        Error ->
          error(Error)
      after
        5000 -> error(timeout)
      end
    end, #{}, lists:seq(1, NumMsgs)),

  _ = timer:sleep(1500),
  {ok, Workers1} = gen_buffer:get_workers(?BUFFER),
  Workers2 = lists:usort(gen_buffer_lib:get_available_workers(?BUFFER)),
  Workers2 = lists:usort(Workers1),
  WorkersLen = maps:size(Replies),
  WorkersLen = length(Workers1),
  WorkersLen = length(Workers2),

  lists:foreach(fun(Worker) ->
    true = maps:get(Worker, Replies) > 1
  end, Workers2).

%%%===================================================================
%%% Internal functions
%%%===================================================================

num_workers(Mod, Buffer) ->
  {ok, Workers} = Mod:get_workers(Buffer),
  length(Workers).
