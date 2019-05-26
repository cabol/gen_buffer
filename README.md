# gen_buffer
> ### High scalable message buffer for Erlang/Elixir
> A generic message buffer behaviour with support for pooling, back-pressure,
  sharding, and distribution.

[![Build Status](https://travis-ci.org/cabol/gen_buffer.svg?branch=master)](https://travis-ci.org/cabol/gen_buffer)

## Overview

The `gen_buffer` can be illustrated as:

```
                              |--[gen_buffer_worker]
                              |
      +--[gen_buffer_sup]--+  |--[gen_buffer_worker]
      |                    |  |
      |    <ets_buffer>    |--|--[gen_buffer_worker] -> <your message handler>
      |                    |  |
      +--------------------+  |--[gen_buffer_worker]
                    ^         |
                    |         |--[gen_buffer_worker]
  (enqueue message) |                   ^
                    |                   | (sends message to worker)
  [gen_buffer]--------------------------+
       ^       (worker available?)
       |
    messages
```

Some implementation notes:

 * A buffer is represented by its own supervision tree. The main supervisor
   creates the buffer itself (using an ETS table) and a pool of workers for it;
   each worker is a `gen_server`.

 * At the moment a message is sent to the buffer, it tries to find an available
   worker (`gen_server`) and then dispatch the message directly to it. If there
   is not any available worker, the message is stored into a buffer data struct
   created and handled using [ets_buffer][ets_buffer]. Once a worker becomes
   available, those buffered messages are processed.

 * In order to get better and/or higher scalability, the buffer can be
   partitioned, it supports sharding under-the-hood. Therefore, in a partitioned
   buffer, the incoming messages are spread across the configured partitions
   (for load balancing); you can configure the desired number of partitions with
   the option `n_partitions` when you start the buffer for the first time.

 * It is also possible to run the buffer in a distributed fashion on multiple
   nodes using the `gen_buffer_dist` module instead.

[ets_buffer]: https://github.com/duomark/epocxy/blob/master/src/ets_buffer.erl

## Installation

### Erlang

In your `rebar.config`:

```erlang
{deps, [
  {gen_buffer, {git, "https://github.com/cabol/gen_buffer.git", {branch, "master"}}}
]}.
```

### Elixir

In your `mix.exs`:

```elixir
def deps do
  [{:gen_buffer, github: "cabol/gen_buffer", branch: "master"}]
end
```

## Usage

### Creating message buffers

First of all, we have to create a message handler module:

```erlang
-module(my_message_handler).

-behaviour(gen_buffer).

%% gen_buffer callbacks
-export([
  init/1,
  handle_message/3
]).

%%%===================================================================
%%% gen_buffer callbacks
%%%===================================================================

%% @hidden
init(_Args) ->
  % initialize your handler state
  {ok, #{}}.

%% @hidden
handle_message(Buffer, Message, State) ->
  % your logic to process incoming messages goes here
  Response = {Buffer, Message},
  {ok, Response, State}.

%% Optionally you can implement `handle_info/3` and `terminate/3`
```

Now we can create our buffer calling `gen_buffer:start_link(BufferName, Opts)`

```erlang
gen_buffer:start_link(my_buffer, #{message_handler => my_message_handler}).
```

It is also possible to start the buffer as part of your supervision tree in your
app. In your supervisor, within the `init/1` callback, you can add the buffer
spec to the supervisor's children list:

```erlang
%% @hidden
init(_) ->
  Children = [
    gen_buffer:child_spec(#{buffer => my_buffer, message_handler => my_message_handler})
  ],

  {ok, {{one_for_one, 0, 1}, Children}}.
```

> You can run `observer:start()` to see how the buffer looks like.

### Options for buffer creation

The following are the options for `gen_buffer:start_link/2`:

Option | Description | Required | Default
:----- | :---------- | :------- | -------
`message_handler` | Message handler module that implements the `gen_buffer` behaviour | YES | NA
`init_args` | Optional arguments passed to `init/1` callback when a worker starts | NO | `undefined`
`workers` | Number of workers | NO | `erlang:system_info(schedulers_online)`
`send_replies` | Determines whether or not to reply with the result of the `handle_message` to the given process when the function `send/2,3` is called | NO | `false`
`buffer_type` | Buffer type according to [ets_buffer][ets_buffer]. Possible values: `ring`, `fifo`, `lifo` | NO | `fifo`
`buffer` | Buffer name. This option is only required for `gen_buffer:child_spec/1` function | NO | NA
`n_partitions` | The number of partitions for the buffer. The load will be balanced across the defined partitions and each partition has its own pool of workers. | NO | `1`

### Sending messages

Messaging is asynchronous by nature, as well as `gen_buffer`, then when you send
a message to a buffer, it is dispatched to another process to be processed
asynchronously via your message handler. If you want to receive the result of
your message handler, you have to start the buffer with `send_replies` option
to `true`. For sending messages we use the function `gen_buffer:send/2,3` as
follows:

```erlang
% if option send_replies has been set to true, the buffer sends the reply to
% the caller process
Ref1 = gen_buffer:send(my_buffer, "hello").

% or you can specify explicitly to what process the buffer should reply to;
% but send_replies has to be set to true
Ref2 = gen_buffer:send(my_buffer, "hello", ReplyToThisPID).
```

### Receiving replies

When we send a message calling `gen_buffer:send/2,3`, a reference is returned
and it can be used to receive the reply, like so:

```erlang
% this is a blocking call that waits for your reply for 5000 milliseconds
% by default. If none reply is received during that time, {error, timeout}
% is returned
gen_buffer:recv(my_buffer, Ref).

% or you can pass the timeout explicitly
gen_buffer:recv(my_buffer, Ref, 1000).
```

### Sending messages and receiving replies in the same call

There is a function `gen_buffer:send_recv/2,3` which combines the previous
two functions in one, meaning that, when you send a message using this function,
it gets blocked until the reply arrives or until the timeout expires.

```erlang
% by default, the timeout is 5000 milliseconds
gen_buffer:send_recv(my_buffer, "hello").

% or you can pass it explicitly
gen_buffer:send_recv(my_buffer, "hello", 1000).
```

### Increase/decrease number of workers dynamically (for throttling purposes)

It is also possible to increase or decrease the number of workers in runtime for
traffic throttling. Controlling the number of workers we can control the
throughput and/or processing rate too.

```erlang
% adding one worker inheriting the initial options at startup time
gen_buffer:incr_worker(my_buffer).

% passing/changing the options
gen_buffer:incr_worker(my_buffer, OtherOpts).

% removing one worker (a random worker)
gen_buffer:decr_worker(my_buffer).

% adding 5 workers at once inheriting the initial options at startup time
gen_buffer:set_workers(my_buffer, 5).

% passing/changing the options
gen_buffer:set_workers(my_buffer, 5, OtherOpts).
```

### Getting buffer info

There are several functions to get info about the buffer, such as:

```erlang
% getting the buffer size (number of buffered messages)
gen_buffer:size(my_buffer).

% getting all available info about the buffer
% check gen_buffer:buffer_info() typespec
gen_buffer:info(my_buffer)

% getting all available info about all created buffers
% check gen_buffer:buffers_info() typespec
gen_buffer:info()

% getting one random worker PID
gen_buffer:get_worker(my_buffer)

% getting all buffer workers (list of PIDs)
gen_buffer:get_worker(my_buffer)
```

## Evaluating message handler logic directly

This is not recommended since it breaks the essence of messaging; remember we
mentioned before the messaging is asynchronous by nature. Nevertheless, there is
a way to execute the `handle_message` logic for a message bypassing the normal
path, it just finds a worker and executing the logic directly with it.

```erlang
% if the evaluation fails, it retries one more time by default
gen_buffer:eval(my_buffer, "hello")

% or you can pass the desired number of retries
gen_buffer:eval(my_buffer, "hello", 5)
```

> The main use cases are for testing, debugging, etc.

## Testing

```
$ make test
```

You can find tests results in `_build/test/logs`, and coverage in
`_build/test/cover`.

> **NOTE:** `gen_buffer` comes with a helper `Makefile`, but it is just a simple
  wrapper on top of `rebar3`, therefore, you can run the tests using `rebar3`
  directly, like so: `rebar3 do ct, cover`.

## Building Edoc

```
$ make doc
```

> **NOTE:** Once you run the previous command, a new folder `doc` is created,
  and you'll have a pretty nice HTML documentation.

## Copyright and License

Copyright (c) 2019 Carlos Bolanos.

**gen_buffer** source code is licensed under the [MIT License](LICENSE.md).
