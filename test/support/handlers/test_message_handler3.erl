-module(test_message_handler3).

-export([
  handle_message/2
]).

handle_message(_BufferName, _Msg) ->
  error(handler_exception).
