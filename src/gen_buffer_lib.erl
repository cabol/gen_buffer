-module(gen_buffer_lib).

%% API
-export([
  get_metadata_value/2,
  get_metadata_value/3,
  get_one_metadata_value/3,
  set_metadata_value/3,
  del_metadata_value/3,
  get_available_workers/1,
  get_available_worker/1,
  get_partition/1,
  get_partition/2,
  partition_name/2,
  concat/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv get_metadata_value(Channel, Key, undefined)
get_metadata_value(Channel, Key) ->
  get_metadata_value(Channel, Key, undefined).

-spec get_metadata_value(
        Channel :: gen_buffer:channel(),
        Key     :: any(),
        Default :: any()
      ) -> any().
get_metadata_value(Channel, Key, Default) ->
  try
    ets:lookup_element(Channel, Key, 2)
  catch
    _:_ -> Default
  end.

-spec get_one_metadata_value(
        Channel :: gen_buffer:channel(),
        Key     :: any(),
        Default :: any()
      ) -> any().
get_one_metadata_value(Channel, Key, Default) ->
  case get_metadata_value(Channel, Key, Default) of
    Value when is_list(Value) ->
      hd(Value);

    Value ->
      Value
  end.

-spec set_metadata_value(
        Channel :: gen_buffer:channel(),
        Key     :: any(),
        Value   :: any()
      ) -> ok.
set_metadata_value(Channel, Key, Value) ->
  _ = ets:insert(Channel, {Key, Value}),
  ok.

-spec del_metadata_value(
        Channel :: gen_buffer:channel(),
        Key     :: any(),
        Value   :: any()
      ) -> ok.
del_metadata_value(Channel, Key, Value) ->
  _ = ets:delete_object(Channel, {Key, Value}),
  ok.

-spec get_available_workers(Channel :: gen_buffer:channel()) -> [pid()].
get_available_workers(Channel) ->
  get_metadata_value(Channel, Channel, []).

-spec get_available_worker(
        Channel :: gen_buffer:channel()
      ) -> pid() | no_available_workers.
get_available_worker(Channel) ->
  case get_available_workers(Channel) of
    [] ->
      no_available_workers;

    Workers ->
      pick_worker(Workers)
  end.

-spec get_partition(Channel :: gen_buffer:channel()) -> gen_buffer:channel().
get_partition(Channel) ->
  NumPartitions = get_one_metadata_value(Channel, n_partitions, 1),
  get_partition(Channel, NumPartitions).

-spec get_partition(
        Channel       :: gen_buffer:channel(),
        NumPartitions :: pos_integer()
      ) -> atom().
get_partition(Channel, NumPartitions) ->
  Partition = erlang:phash2(os:timestamp(), NumPartitions),
  partition_name(Channel, Partition).

-spec partition_name(
        Channel   :: gen_buffer:channel(),
        Partition :: non_neg_integer()
      ) -> atom().
partition_name(Channel, Partition) ->
  Bin = <<(atom_to_binary(Channel, utf8))/binary, ".", (integer_to_binary(Partition))/binary>>,
  binary_to_atom(Bin, utf8).

-spec concat(term(), term()) -> atom().
concat(Term1, Term2) ->
  list_to_atom(lists:flatten(io_lib:format("~p.~p", [Term1, Term2]))).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
pick_worker([]) ->
  no_available_workers;

pick_worker([Worker | Rest]) ->
  case process_info(Worker, message_queue_len) of
    {message_queue_len, Len} when Len > 0 ->
      pick_worker(Rest);

    _ ->
      Worker
  end.
