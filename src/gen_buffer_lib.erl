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
  partition_name/2
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @equiv get_metadata_value(Buffer, Key, undefined)
get_metadata_value(Buffer, Key) ->
  get_metadata_value(Buffer, Key, undefined).

-spec get_metadata_value(Buffer :: gen_buffer:t(), Key :: any(), Default :: any()) -> any().
get_metadata_value(Buffer, Key, Default) ->
  try
    ets:lookup_element(Buffer, Key, 2)
  catch
    _:_ -> Default
  end.

-spec get_one_metadata_value(Buffer :: gen_buffer:t(), Key :: any(), Default :: any()) -> any().
get_one_metadata_value(Buffer, Key, Default) ->
  case get_metadata_value(Buffer, Key, Default) of
    Value when is_list(Value) ->
      hd(Value);

    Value ->
      Value
  end.

-spec set_metadata_value(Buffer :: gen_buffer:t(), Key :: any(), Value :: any()) -> ok.
set_metadata_value(Buffer, Key, Value) ->
  _ = ets:insert(Buffer, {Key, Value}),
  ok.

-spec del_metadata_value(Buffer :: gen_buffer:t(), Key :: any(), Value :: any()) -> ok.
del_metadata_value(Buffer, Key, Value) ->
  _ = ets:delete_object(Buffer, {Key, Value}),
  ok.

-spec get_available_workers(Buffer :: gen_buffer:t()) -> [pid()].
get_available_workers(Buffer) ->
  get_metadata_value(Buffer, Buffer, []).

-spec get_available_worker(Buffer :: gen_buffer:t()) -> pid() | no_available_workers.
get_available_worker(Buffer) ->
  case get_available_workers(Buffer) of
    [] ->
      no_available_workers;

    Workers ->
      pick_worker(Workers)
  end.

-spec get_partition(Buffer :: gen_buffer:t()) -> gen_buffer:t().
get_partition(Buffer) ->
  NumPartitions = get_one_metadata_value(Buffer, n_partitions, 1),
  get_partition(Buffer, NumPartitions).

-spec get_partition(Buffer :: gen_buffer:t(), NumPartitions :: pos_integer()) -> atom().
get_partition(Buffer, NumPartitions) ->
  Partition = erlang:phash2(os:timestamp(), NumPartitions),
  partition_name(Buffer, Partition).

-spec partition_name(Buffer :: gen_buffer:t(), Partition :: non_neg_integer()) -> atom().
partition_name(Buffer, Partition) ->
  Bin = <<(atom_to_binary(Buffer, utf8))/binary, ".", (integer_to_binary(Partition))/binary>>,
  binary_to_atom(Bin, utf8).

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
