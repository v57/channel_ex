defmodule ChannelEx.RequestHandle do
  @moduledoc false
  defstruct [:sender, :rid, :ref, :kind]
end

defmodule ChannelEx.Cancellable do
  @moduledoc false
  defstruct [:cancel]
end

defmodule ChannelEx.Sender do
  @moduledoc false
  import Kernel, except: [send: 2]

  alias ChannelEx.Message

  defstruct [:id, :channel, :owner, :transport, :state_ref]

  def new(channel, owner, transport, state_ref, inline \\ nil) do
    _ = inline

    %__MODULE__{
      id: make_ref(),
      channel: channel,
      owner: owner,
      transport: transport,
      state_ref: state_ref
    }
  end

  def send(%__MODULE__{} = sender, path, body \\ nil, context \\ nil, timeout \\ 5_000) do
    if self() == sender.owner and sender.transport == :process and sender.state_ref do
      _ = timeout
      ChannelEx.Core.invoke_post(sender.channel, sender, sender.state_ref, path, body, context)
    else
      request = request(sender, path, body, context)
      await(request, timeout)
    end
  end

  def request(%__MODULE__{} = sender, path, body \\ nil, context \\ nil) when is_binary(path) do
    owner = self()
    ref = make_ref()

    Kernel.send(
      sender.owner,
      {:channel_ex_request, owner, ref, path, body, context}
    )

    %ChannelEx.RequestHandle{sender: sender, rid: nil, ref: ref, kind: :request}
  end

  def await(%ChannelEx.RequestHandle{} = request, timeout \\ 5_000) do
    ref = request.ref

    receive do
      {^ref, response} ->
        if Message.get(response, :error) do
          {:error, Message.get(response, :error)}
        else
          {:ok, Message.get(response, :body)}
        end
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def cancel(%ChannelEx.RequestHandle{} = request) do
    Kernel.send(request.sender.owner, {:channel_ex_cancel_request, request.ref})
    :ok
  end

  def values(%__MODULE__{} = sender, path, body \\ nil, context \\ nil) do
    ChannelEx.Values.stream(sender, path, body, context)
  end

  def subscribe(%__MODULE__{} = sender, path, body, event_fun, context \\ nil, timeout \\ 5_000)
      when is_binary(path) and is_function(event_fun, 1) do
    owner = self()
    ref = make_ref()

    Kernel.send(
      sender.owner,
      {:channel_ex_subscribe, owner, ref, path, body, context, event_fun}
    )

    receive do
      {^ref, {:ok, subscription_id}} ->
        {:ok,
         %ChannelEx.Cancellable{
           cancel: fn ->
             Kernel.send(sender.owner, {:channel_ex_cancel_subscription, subscription_id})
             :ok
           end
         }}

      {^ref, {:error, error}} ->
        {:error, error}
    after
      timeout ->
        {:error, :timeout}
    end
  end

  def stop(%__MODULE__{} = sender) do
    Kernel.send(sender.owner, :channel_ex_stop)
    :ok
  end
end
