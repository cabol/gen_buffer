-module(gen_buffer_common_test_cases).

-include_lib("common_test/include/ct.hrl").

%% Test Cases
-export([
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
]).

%% Helpers
-export([
  create_channel/4
]).

-define(CHANNEL, gen_buffer_test).

%%%===================================================================
%%% Tests Cases
%%%===================================================================

t_eval(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  "hello" = Mod:eval(?CHANNEL, "hello"),
  1 = Mod:eval(?CHANNEL, 1),
  {ok, "hello"} = Mod:eval(?CHANNEL, {ok, "hello"}),

  try Mod:eval(?CHANNEL, error)
  catch
    exit:{{handler_exception, _}, _} -> ok
  end.

t_eval_error(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts#{workers => 0}, Mod, Config),

  no_available_workers = Mod:eval(?CHANNEL, "hello", 5),

  true = register(ct, self()),
  _ = process_flag(trap_exit, true),

  Pid =
    spawn_link(fun() ->
      Res = Mod:eval(?CHANNEL, "hello", 0),
      ct ! Res
    end),

  {error, timeout} = gen_buffer_ct:wait_for_msg(1000),
  true = unregister(ct),
  true = exit(Pid, normal).

t_send_recv(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  Ref1 = Mod:send(?CHANNEL, "hello"),
  {reply, Ref1, ?CHANNEL, "hello"} = gen_buffer_ct:wait_for_msg(),

  Ref2 = Mod:send(?CHANNEL, "hello"),
  {ok, "hello"} = Mod:recv(?CHANNEL, Ref2).

t_send_recv_error(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  _ = Mod:send(?CHANNEL, error),
  {error, _, ?CHANNEL, handler_exception} = gen_buffer_ct:wait_for_msg(1000),

  _ = create_channel(test, Opts#{send_replies => false}, Mod, Config),

  Ref = Mod:send(test, error),
  {error, timeout} = Mod:recv(test, Ref, 1000).

t_sync_send_recv(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  {ok, "hello"} = Mod:sync_send_recv(?CHANNEL, "hello"),

  Opts1 = Opts#{message_handler => test_message_handler4},
  _ = create_channel(test, Opts1, Mod, Config),

  {error, handler_exception} = Mod:sync_send_recv(test, "hello"),

  Opts2 = Opts#{message_handler => test_message_handler4, send_replies => false},
  _ = create_channel(test2, Opts2, Mod, Config),

  {error, timeout} = Mod:sync_send_recv(test2, "hello", 1000).

t_fire_and_forget(Config) ->
  Mod = ?config(module, Config),
  Opts = (?config(opts, Config))#{message_handler => test_message_handler4, send_replies => false},
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  Ref = Mod:send(?CHANNEL, "hello"),
  {error, timeout} = Mod:recv(?CHANNEL, Ref, 500),

  _ = create_channel(test, Opts#{message_handler => test_message_handler4}, Mod, Config),

  Ref2 = Mod:send(test, {self(), "hello"}),
  {error, timeout} = Mod:recv(?CHANNEL, Ref2, 500).

t_add_del_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = maps:to_list(?config(opts, Config)),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),
  Workers = num_workers(Mod, ?CHANNEL),

  ok = lists:foreach(fun(_) ->
    Mod:incr_worker(?CHANNEL)
  end, lists:seq(1, 2)),

  ok = lists:foreach(fun(_) ->
    Mod:incr_worker(?CHANNEL, Opts)
  end, lists:seq(3, 5)),

  Workers1 = num_workers(Mod, ?CHANNEL),
  Workers1 = Workers + 5,

  ok = Mod:decr_worker(?CHANNEL),
  ok = Mod:decr_worker(wrong_channel),

  Workers2 = num_workers(Mod, ?CHANNEL),
  Workers2 = Workers + 4,

  ok = lists:foreach(fun(_) ->
    Mod:decr_worker(?CHANNEL)
  end, lists:seq(1, Workers2)),

  {error, no_available_channel} = Mod:get_worker(wrong_channel),
  {error, no_available_channel} = Mod:get_workers(wrong_channel),
  {error, no_available_channel} = Mod:set_workers(wrong_channel, 10, Opts),
  {error, no_available_workers} = Mod:decr_worker(?CHANNEL),
  {error, no_available_workers} = Mod:poll(?CHANNEL).

t_set_workers(Config) ->
  Mod = ?config(module, Config),
  Opts = maps:to_list(?config(opts, Config)),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),
  Workers = num_workers(Mod, ?CHANNEL),

  {ok, WorkersL1} = Mod:set_workers(?CHANNEL, Workers + 2),
  Workers1 = length(WorkersL1),
  Workers1 = Workers + 2,

  {ok, WorkersL2} = Mod:set_workers(?CHANNEL, Workers1 - 3, Opts),
  Workers2 = length(WorkersL2),
  Workers2 = Workers - 1,

  try
    Mod:set_workers(?CHANNEL, -1, Opts)
  catch
    error:function_clause -> ok
  end.

t_queue_size(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts#{workers => 0}, Mod, Config),

  0 = Mod:queue_size(?CHANNEL),

  ok = lists:foreach(fun(_) ->
    Mod:send(?CHANNEL, "hello")
  end, lists:seq(1, 10)),

  10 = Mod:queue_size(?CHANNEL),
  undefined = Mod:queue_size(undefined).

t_info_channel(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),

  #{workers := W, queue_size := QS} = Mod:info(?CHANNEL),
  true = is_integer(W),
  true = is_integer(QS),
  undefined = Mod:info(undefined).

t_info(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts, Mod, Config),
  _ = create_channel(test, Opts, Mod, Config),
  _ = create_channel(test2, Opts#{workers => 0}, Mod, Config),
  ok = pg2:create(yet_another_group),

  InfoMap = Mod:info(),
  3 = map_size(InfoMap),

  #{
    ?CHANNEL := #{workers := W, queue_size := QS},
    test     := #{workers := _, queue_size := _},
    test2    := #{workers := 0, queue_size := _}
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
  _ = create_channel(?CHANNEL, Opts1, Mod, Config),

  0 = Mod:queue_size(?CHANNEL),

  ok = lists:foreach(fun(_) ->
    Mod:send(?CHANNEL, "hello")
  end, lists:seq(1, 5)),

  5 = Mod:queue_size(?CHANNEL),

  _ = Mod:set_workers(?CHANNEL, 1),
  _ = timer:sleep(500),
  _ = Mod:poll(?CHANNEL),

  ok = lists:foreach(fun(_) ->
    "hello" = gen_buffer_ct:wait_for_msg(1000)
  end, lists:seq(1, 5)),

  0 = Mod:queue_size(?CHANNEL),
  true = unregister(ct).

t_worker_distribution(Config) ->
  Mod = ?config(module, Config),
  Opts = ?config(opts, Config),
  _ = create_channel(?CHANNEL, Opts#{message_handler => test_message_handler5}, Mod, Config),

  {ok, CurrentWorkers} = gen_buffer:get_workers(?CHANNEL),
  NumMsgs = length(CurrentWorkers) * 100,
  Refs = [Mod:send(?CHANNEL, M) || M <- lists:seq(1, NumMsgs)],
  no_available_workers = gen_buffer_lib:get_available_worker(?CHANNEL),

  Replies =
    lists:foldl(fun(_, Acc) ->
      receive
        {reply, Ref, ?CHANNEL, {_, Worker}} ->
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
  {ok, Workers1} = gen_buffer:get_workers(?CHANNEL),
  Workers2 = lists:usort(gen_buffer_lib:get_available_workers(?CHANNEL)),
  Workers2 = lists:usort(Workers1),
  WorkersLen = maps:size(Replies),
  WorkersLen = length(Workers1),
  WorkersLen = length(Workers2),

  lists:foreach(fun(Worker) ->
    true = maps:get(Worker, Replies) > 1
  end, Workers2).

%%%===================================================================
%%% Helpers
%%%===================================================================

create_channel(Channel, Opts, gen_buffer_dist, Config) ->
  {ok, _} = gen_buffer_dist:start_link(Channel, Opts),
  Nodes = ?config(nodes, Config),
  Parent = {ct, 'ct@127.0.0.1'},

  Fun = fun() ->
    Name = list_to_atom("parent_" ++ atom_to_list(Channel)),
    _ = register(Name, self()),

    case gen_buffer:start_link(Channel, Opts) of
      {ok, _} ->
        ok;

      {error, {already_started, _}} ->
        ok = gen_buffer:start_link(Channel),
        {ok, _} = gen_buffer:start_link(Channel, Opts)
    end,

    receive
      exit -> Parent ! {exit, Name}
    end
  end,

  {ResL, []} = rpc:multicall(Nodes, erlang, spawn_link, [Fun]),
  _ = timer:sleep(1000),
  {ok, ResL};

create_channel(Channel, Opts, Mod, _) ->
  Mod:start_link(Channel, Opts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

num_workers(Mod, Channel) ->
  {ok, Workers} = Mod:get_workers(Channel),
  length(Workers).
