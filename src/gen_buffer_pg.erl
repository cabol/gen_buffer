%%%-------------------------------------------------------------------
%%% @doc
%%% This module provides an abstraction for `pg' (OTP 23 or higher)
%%% and `pg2' (OTP 22 or lower).
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer_pg).

%% API
-export([
  create/1,
  delete/1,
  join/1,
  join/2,
  leave/1,
  leave/2,
  get_members/1,
  which_groups/0
]).

%%%===================================================================
%%% API
%%%===================================================================

-ifndef(OTP_RELEASE).
  %% For OTP 20 or lower ensure OTP_RELEASE; since this macro was
  %% introduced in OTP release 21.
  -define(OTP_RELEASE, 20).
-endif.

-if(?OTP_RELEASE >= 23).
%% PG
-export([child_spec/0]).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
  #{
    id => ?MODULE,
    start => {pg, start_link, [?MODULE]}
  }.

-spec join(Tab :: atom(), Pid :: pid()) -> ok.
join(Tab, Pid) when is_atom(Tab), is_pid(Pid) ->
  pg:join(?MODULE, Tab, Pid).

-spec leave(Tab :: atom(), Pid :: pid()) -> ok.
leave(Tab, Pid) when is_atom(Tab), is_pid(Pid) ->
  pg:leave(?MODULE, Tab, Pid).

-spec get_members(Tab :: atom()) -> [pid()].
get_members(Tab) when is_atom(Tab) ->
  pg:get_members(?MODULE, Tab).

-spec which_groups() -> [pg:group()].
which_groups() ->
  pg:which_groups(?MODULE).

-spec create(Buf :: atom()) -> ok.
create(_Buf) ->
  ok.

-spec delete(Buf :: atom()) -> ok.
delete(_Buf) ->
  ok.

-else.
%% PG2

-spec join(Tab :: atom(), Pid :: pid()) -> ok.
join(Tab, Pid) when is_atom(Tab), is_pid(Pid) ->
  pg2:join(ensure_namespace(Tab), Pid).

-spec leave(Tab :: atom(), Pid :: pid()) -> ok.
leave(Tab, Pid) when is_atom(Tab), is_pid(Pid) ->
  pg2:leave(ensure_namespace(Tab), Pid).

-spec get_members(Tab :: atom()) -> [pid()].
get_members(Tab) when is_atom(Tab) ->
  pg2:get_members(ensure_namespace(Tab)).

-spec which_groups() -> [pg2:group()].
which_groups() ->
  pg2:which_groups().

-spec create(Buf :: atom()) -> ok.
create(Buf) ->
  ok = pg2:create(ensure_namespace(Buf)),
  ok.

-spec delete(Buf :: atom()) -> ok.
delete(Buf) ->
  pg2:delete(ensure_namespace(Buf)).

%% @private
ensure_namespace(Tab) ->
  Namespace = {gen_buffer, Tab},
  ok = pg2:create(Namespace),
  Namespace.

-endif.

%% @equiv join(Tab, shards_meta:tab_pid(Tab))
join(Tab) when is_atom(Tab) ->
  join(Tab, shards_meta:tab_pid(Tab)).

%% @equiv leave(Tab, shards_meta:tab_pid(Tab))
leave(Tab) when is_atom(Tab) ->
  leave(Tab, shards_meta:tab_pid(Tab)).

