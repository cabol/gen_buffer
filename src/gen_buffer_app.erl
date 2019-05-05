%%%-------------------------------------------------------------------
%%% @doc
%%% gen_buffer app
%%% @end
%%%-------------------------------------------------------------------
-module(gen_buffer_app).

-behaviour(application).
-behaviour(supervisor).

%% Application callbacks
-export([
  start/2,
  stop/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SUPERVISOR, gen_buffer_sup).

%%%===================================================================
%%% API
%%%===================================================================

%% @hidden
start(_StartType, _StartArgs) ->
  supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

%% @hidden
stop(_State) ->
  ok.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%% @hidden
init(_) ->
  {ok, {{one_for_all, 0, 1}, []}}.
