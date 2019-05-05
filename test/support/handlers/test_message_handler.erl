-module(test_message_handler).

-behaviour(gen_buffer).

-export([
  init/1,
  handle_message/3,
  handle_info/3,
  terminate/3
]).

init({test, Result}) ->
  Result;
init(_Args) ->
  _ = process_flag(trap_exit, true),
  {ok, #{messages => []}}.

handle_message(_Channel, error, _State) ->
  error(handler_exception);
handle_message(_Channel, messages, #{messages := MsgL} = State) ->
  {ok, MsgL, State};
handle_message(_Channel, Msg, #{messages := MsgL} = State) ->
  {ok, Msg, State#{messages := [Msg | MsgL]}}.

handle_info(_Channel, {'EXIT', _From, Reason}, State) ->
  {stop, Reason, State};
handle_info(_Channel, throw, State) ->
  throw({stop, throw, State});
handle_info(_Channel, Info, _State) ->
  Info.

terminate(_Channel, throw, _State) ->
  throw(ok);
terminate(_Channel, _Info, _State) ->
  ok.
