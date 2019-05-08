-module(test_message_handler2).

-behaviour(gen_buffer).

-export([
  init/1,
  handle_message/3,
  handle_info/3
]).

init(_Args) ->
  _ = process_flag(trap_exit, true),
  {ok, #{}}.

handle_message(_BufferName, _Msg, _State) ->
  error(handler_exception).

handle_info(_Buffer, _Info, _State) ->
  throw(handler_exception).
