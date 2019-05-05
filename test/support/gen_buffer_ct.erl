-module(gen_buffer_ct).

-export([
  wait_for_msg/0,
  wait_for_msg/1,
  messages/0,
  messages/1
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
