defmodule ChannelEx.ServerListener do
  @moduledoc false

  defstruct [:owner, :channel, :port]

  def stop(%__MODULE__{} = listener) do
    send(listener.owner, :channel_ex_stop)
    :ok
  end
end

defmodule ChannelEx.Server do
  @moduledoc false

  def listen(channel, address, options \\ []) do
    {:ok, owner} = ChannelEx.ServerOwner.start_link(channel, address, options)

    %ChannelEx.ServerListener{
      owner: owner,
      channel: channel,
      port: ChannelEx.ServerOwner.port(owner)
    }
  end
end
