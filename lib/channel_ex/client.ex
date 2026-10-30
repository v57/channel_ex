defmodule ChannelEx.ClientSender do
  @moduledoc false

  defstruct [:sender, :ws]

  def send(%__MODULE__{sender: sender}, path, body \\ nil, context \\ nil, timeout \\ 5_000) do
    ChannelEx.Sender.send(sender, path, body, context, timeout)
  end

  def request(%__MODULE__{sender: sender}, path, body \\ nil, context \\ nil) do
    ChannelEx.Sender.request(sender, path, body, context)
  end

  def await(request, timeout \\ 5_000), do: ChannelEx.Sender.await(request, timeout)
  def cancel(request), do: ChannelEx.Sender.cancel(request)

  def values(%__MODULE__{sender: sender}, path, body \\ nil, context \\ nil) do
    ChannelEx.Sender.values(sender, path, body, context)
  end

  def subscribe(%__MODULE__{sender: sender}, path, body, event_fun, context \\ nil) do
    ChannelEx.Sender.subscribe(sender, path, body, event_fun, context, 5_000)
  end

  def stop(%__MODULE__{sender: sender}), do: ChannelEx.Sender.stop(sender)
  def throttle(%__MODULE__{ws: socket}), do: ChannelEx.Client.Socket.throttle(socket)
end

defmodule ChannelEx.Client do
  @moduledoc false

  def connect(channel, port) when is_integer(port), do: connect(channel, port, [])
  def connect(channel, address) when is_binary(address), do: connect(channel, address, [])

  def connect(channel, port, options) when is_integer(port) do
    connect(channel, "ws://127.0.0.1:#{port}", options)
  end

  @doc """
  Connects and returns a `%ChannelEx.ClientSender{}`, or `{:error, reason}` when the TCP
  connection or the websocket handshake fails (refused, non-101 reply, silent server).
  The caller is linked to the socket process only after a successful handshake, so a
  failed connect never crashes the caller.
  """
  def connect(channel, "quic://" <> _ = address, options) do
    ChannelEx.Quic.connect(channel, address, options)
  end

  def connect(channel, address, options) when is_binary(address) do
    opts = [
      channel: channel,
      url: address,
      headers: Keyword.get(options, :headers),
      on_connect: Keyword.get(options, :on_connect),
      state_ref: Keyword.get(options, :state, %{}),
      connect_timeout: Keyword.get(options, :connect_timeout, 5_000),
      max_payload: Keyword.get(options, :max_payload, ChannelEx.WebSocket.default_max_payload())
    ]

    with {:ok, socket, rest} <- ChannelEx.Client.Socket.open(opts),
         {:ok, pid} <-
           ChannelEx.Client.Socket.start_link(opts ++ [socket: socket, recv_buffer: rest]) do
      case ChannelEx.Client.Socket.attach(pid, socket) do
        :ok ->
          try do
            %ChannelEx.ClientSender{sender: ChannelEx.Client.Socket.sender(pid), ws: pid}
          catch
            # The server closed the connection right after the handshake.
            :exit, _reason -> {:error, :closed}
          end

        {:error, _reason} = error ->
          ChannelEx.Client.Socket.stop(pid)
          error
      end
    end
  end
end
