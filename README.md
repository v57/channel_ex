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
fallback encoder. The WebSocket client and server are written on `:gen_tcp`. `quicer` is optional, for the QUIC
transport only (see below). `excoveralls` is for the tests only.

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

# QUIC transport (experimental)

The same messages over QUIC: one bidirectional stream per connection, each message prefixed with its
length (wire format in `../channel/docs/quic.md`). It runs on [quicer](https://hex.pm/packages/quicer)
(Erlang bindings to msquic), an **optional** dependency: an app that uses only WebSocket never fetches or
builds it. To use QUIC, add it next to `channel_ex` (its build compiles msquic, so it needs `cmake` and a C
compiler):

```elixir
def deps do
  [
    {:channel_ex, path: "../channel_ex"},
    {:quicer, "~> 0.4.12"}
  ]
end
```

```elixir
# Server: QUIC always needs a key and a certificate (PEM)
listener =
  ChannelEx.new()
  |> ChannelEx.post("hello", fn _ -> "world" end)
  |> ChannelEx.listen_quic(4433, key: File.read!("key.pem"), cert: File.read!("cert.pem"))
# listen_quic takes the same :state, :on_connect, :on_disconnect and :max_payload options as listen

# Client: connect accepts quic:// addresses
client = ChannelEx.connect(ChannelEx.new(), "quic://127.0.0.1:4433", quic: [ca: File.read!("cert.pem")])
{:ok, "world"} = ChannelEx.send(client, "hello")
```

Certificates (the `quic:` options of `connect`):
- a certificate trusted by the system (for example Let's Encrypt): nothing to set;
- `ca: pem` trusts that certificate or CA, for self-signed servers. The address must be in the certificate's
  subject alternative names, as a DNS name or an IP address;
- `fingerprint: "DD:D2:37:…"` accepts only the certificate with that SHA-256 fingerprint (hex, colons
  optional) and skips the other checks;
- `servername: "example.com"` overrides the name sent and checked (it defaults to the address host).

Each client connection dials from its own UDP socket, never one shared endpoint (Network.framework servers
such as HubServer take one connection per client address and port). The client sends a keepalive every 15 s
and both sides close a connection after 30 s of silence. Like this port's WebSocket client, the QUIC client
doesn't reconnect: when the connection drops, pending calls fail with `"disconnected"`.

`ChannelEx.Quic.available?/0` tells whether the build has QUIC. For the languages that support QUIC, see
`channel/docs/quic.md`.

# Implementation

- client and server WebSocket transports, written on `:gen_tcp`
- client and server QUIC transports (experimental, optional `:quicer`)
- process (in-memory) transport
- fast JSON (`:jiffy`, with `Jason` fallback)

# Requirements
- Elixir 1.19+
- Erlang/OTP 28+
- Bun (for TypeScript interop tests/benchmarks)

# Scripts
- `scripts/test` runs all tests (including interop against `../channel/tests/server.ts` on `127.0.0.1:2049`;
  the QUIC tests start their own fixture on `CHANNEL_TEST_PORT`, 2104 by default)
- `scripts/benchmark` runs the staged websocket benchmark (`bench/benchmark.exs`) and writes `benchmark.json`
- `scripts/coverage` runs tests with coverage and writes LCOV to `coverage/lcov.info`

# Notes
- Protocol semantics are aligned with `../channel/channel.ts`.
- `ObjectMap` is not ported directly; native Elixir maps/agents are used.
