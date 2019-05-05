-module(test_message_handler3).

-export([
  handle_message/2
]).

handle_message(_ChannelName, _Msg) ->
  error(handler_exception).
