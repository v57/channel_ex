defmodule ChannelEx.Process do
  @moduledoc false

  def process(channel, options \\ []) do
    initial_state = Keyword.get(options, :state, %{})
    {:ok, owner} = ChannelEx.ProcessOwner.start_link(channel, initial_state)
    ChannelEx.ProcessOwner.sender(owner)
  end
end
