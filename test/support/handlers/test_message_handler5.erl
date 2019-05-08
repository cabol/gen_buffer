-module(test_message_handler5).

-behaviour(gen_buffer).

-export([
  handle_message/3
]).

handle_message(_Buffer, Msg, State) ->
  _ = timer:sleep(500),
  {ok, {Msg, self()}, State}.
