<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Elixir implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Streaming values are supported out of the box.

# Installation

```elixir
def deps do
  [
    {:channel_ex, path: "../channel_ex"}
  ]
end
```

# Dependencies

[jiffy](https://hex.pm/packages/jiffy) (fast JSON, a C NIF) with [jason](https://hex.pm/packages/jason) as the
fallback encoder. The WebSocket client and server are written on `:gen_tcp`. `excoveralls` is for the tests only.

# Usage

```elixir
# Server
channel =
  ChannelEx.new()
  |> ChannelEx.post("hello", fn _ -> "world" end)

listener = ChannelEx.listen(channel, 8080)
```

```elixir
# Client
client = ChannelEx.connect(ChannelEx.new(), 8080)
{:ok, "world"} = ChannelEx.send(client, "hello")

client
|> ChannelEx.values("numbers")
|> Enum.each(&IO.inspect/1)

ChannelEx.ClientSender.stop(client)
ChannelEx.ServerListener.stop(listener)
```

```elixir
# In-memory process transport
sender =
  ChannelEx.new()
  |> ChannelEx.post("ping", fn _ -> "pong" end)
  |> ChannelEx.process()

{:ok, "pong"} = ChannelEx.send(sender, "ping")
```

# Implementation

- client and server WebSocket transports, written on `:gen_tcp`
- process (in-memory) transport
- fast JSON (`:jiffy`, with `Jason` fallback)

# Requirements
- Elixir 1.19+
- Erlang/OTP 28+
- Bun (for TypeScript interop tests/benchmarks)

# Scripts
- `scripts/test` runs all tests (including interop against `../channel/tests/server.ts` on `127.0.0.1:2049`)
- `scripts/benchmark` runs the staged websocket benchmark (`bench/benchmark.exs`) and writes `benchmark.json`
- `scripts/coverage` runs tests with coverage and writes LCOV to `coverage/lcov.info`

# Notes
- Protocol semantics are aligned with `../channel/channel.ts`.
- `ObjectMap` is not ported directly; native Elixir maps/agents are used.
