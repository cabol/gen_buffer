-module(test_message_handler4).

-behaviour(gen_buffer).

-export([
  handle_message/3,
  handle_info/3
]).

handle_message(_BufferName, _Msg, _State) ->
  error(handler_exception).

handle_info(_Buffer, Info, _State) ->
  _ = timer:sleep(5000),
  {noreply, Info}.
