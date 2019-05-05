-module(test_message_handler6).

-behaviour(gen_buffer).

-export([
  handle_message/3
]).

handle_message(_Channel, Msg, State) ->
  ct ! Msg,
  {ok, Msg, State}.
