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

  def connect(channel, address, options) when is_binary(address) do
    {:ok, socket} =
      ChannelEx.Client.Socket.start_link(
        channel: channel,
        url: address,
        headers: Keyword.get(options, :headers),
        on_connect: Keyword.get(options, :on_connect),
        state_ref: Keyword.get(options, :state, %{})
      )

    sender = ChannelEx.Client.Socket.sender(socket)
    %ChannelEx.ClientSender{sender: sender, ws: socket}
  end
end
