defmodule ChannelEx.Quic do
  @moduledoc """
  QUIC transport (experimental): the same messages as the WebSocket transport over one QUIC
  stream per connection, each prefixed with its length (`channel/docs/quic.md` in the TypeScript
  reference).

  It needs the optional `:quicer` dependency (Erlang bindings to msquic). Without it this module
  is all that's compiled, and `connect/3` and `listen/3` report that QUIC isn't available.

  Client options (`ChannelEx.connect(channel, "quic://host:port", quic: [...])`):
  - `:ca` - PEM certificate (or CA) to trust, for self-signed servers
  - `:fingerprint` - SHA-256 of the server certificate in hex (colons optional); pins it
    instead of verifying it
  - `:servername` - the name sent and checked, instead of the address host

  Server options (`ChannelEx.listen_quic(channel, 4433, key: pem, cert: pem)`): `:key` and
  `:cert` (PEM), plus `listen/3`'s `:state`, `:on_connect`, `:on_disconnect` and `:max_payload`.
  """

  @doc "Whether this build has QUIC (the optional `:quicer` dependency is installed)."
  def available?, do: Code.ensure_loaded?(ChannelEx.Quic.Client)

  @doc false
  def connect(channel, address, options) do
    if available?() do
      # apply/3: the module isn't compiled without quicer
      apply(ChannelEx.Quic.Client, :connect, [channel, address, options])
    else
      {:error, :quic_unavailable}
    end
  end

  @doc false
  def listen(channel, address, options) do
    if available?() do
      apply(ChannelEx.Quic.Server, :listen, [channel, address, options])
    else
      raise ArgumentError,
            "QUIC needs the optional :quicer dependency: add {:quicer, \"~> 0.4.12\"} to your deps"
    end
  end
end
