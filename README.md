# gen_buffer
> ### Partitioned. Distributed. Fast.
> A generic message buffer behaviour with back-pressure for Erlang/Elixir.

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
