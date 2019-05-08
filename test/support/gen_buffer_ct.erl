-module(gen_buffer_ct).

-export([
  wait_for_msg/0,
  wait_for_msg/1,
  messages/0,
  messages/1,
  create_buffer/4
]).

%%%===================================================================
%%% API
%%%===================================================================

wait_for_msg() ->
  wait_for_msg(infinity).

wait_for_msg(Timeout) ->
  receive
    Msg -> Msg
  after
    Timeout -> {error, timeout}
  end.

messages() ->
  messages(self()).

messages(Pid) ->
  {messages, Messages} = erlang:process_info(Pid, messages),
  Messages.

create_buffer(Buffer, Opts, gen_buffer_dist, Config) ->
  {ok, _} = gen_buffer_dist:start_link(Buffer, Opts),
  {nodes, Nodes} = lists:keyfind(nodes, 1, Config),
  Parent = {ct, 'ct@127.0.0.1'},

  Fun = fun() ->
    Name = list_to_atom("parent_" ++ atom_to_list(Buffer)),
    _ = register(Name, self()),

    case gen_buffer:start_link(Buffer, Opts) of
      {ok, _} ->
        ok;

      {error, {already_started, _}} ->
        ok = gen_buffer:start_link(Buffer),
        {ok, _} = gen_buffer:start_link(Buffer, Opts)
    end,

    receive
      exit -> Parent ! {exit, Name}
    end
  end,

  {ResL, []} = rpc:multicall(Nodes, erlang, spawn_link, [Fun]),
  _ = timer:sleep(1000),
  {ok, ResL};

create_buffer(Buffer, Opts, Mod, _) ->
  Mod:start_link(Buffer, Opts).
