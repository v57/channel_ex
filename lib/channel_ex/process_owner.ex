defmodule ChannelEx.ProcessOwner do
  @moduledoc false

  use GenServer

  def start_link(channel, initial_state) do
    GenServer.start_link(__MODULE__, {channel, initial_state})
  end

  def sender(pid), do: GenServer.call(pid, :sender)

  @impl true
  def init({channel, initial_state}) do
    owner = self()
    {:ok, state_ref} = ChannelEx.ConnectionState.start_link(initial_state)
    sender = ChannelEx.Sender.new(channel, owner, :process, state_ref)

    publisher_refs =
      ChannelEx.Core.register_publishers(channel, fn event ->
        Kernel.send(owner, {:channel_ex_subscription_event, event.topic, event.body})
      end)

    {:ok,
     %{
       channel: channel,
       state_ref: state_ref,
       sender: sender,
       next_sub_id: 0,
       remote_topics: MapSet.new(),
       subscriptions: %{},
       callbacks: %{},
       publisher_refs: publisher_refs
     }}
  end

  @impl true
  def handle_call(:sender, _from, state), do: {:reply, state.sender, state}

  @impl true
  def handle_info({:channel_ex_request, caller, ref, path, body, context}, state) do
    send(
      caller,
      {ref,
       response_map(
         ChannelEx.Core.invoke_post(
           state.channel,
           state.sender,
           state.state_ref,
           path,
           body,
           context
         )
       )}
    )

    {:noreply, state}
  end

  def handle_info({:channel_ex_stream_request, caller, ref, path, body, context}, state) do
    case ChannelEx.Core.invoke_stream(
           state.channel,
           state.sender,
           state.state_ref,
           path,
           body,
           context
         ) do
      {:ok, values} ->
        Enum.each(values, fn value ->
          send(caller, {ref, %{body: value}})
        end)

        send(caller, {ref, %{done: true}})

      {:error, error} ->
        send(caller, {ref, %{error: error}})
    end

    {:noreply, state}
  end

  def handle_info({:channel_ex_subscribe, caller, ref, path, body, _context, event_fun}, state) do
    case ChannelEx.Core.invoke_subscribe(state.channel, path, body) do
      {:ok, topic, _response_body} ->
        sub_id = state.next_sub_id + 1

        callbacks =
          Map.update(state.callbacks, topic, %{sub_id => event_fun}, fn topic_callbacks ->
            Map.put(topic_callbacks, sub_id, event_fun)
          end)

        subscriptions = Map.put(state.subscriptions, sub_id, topic)
        remote_topics = ChannelEx.Core.remote_topics_add(state.remote_topics, topic)
        send(caller, {ref, {:ok, sub_id}})

        {:noreply,
         %{
           state
           | next_sub_id: sub_id,
             callbacks: callbacks,
             subscriptions: subscriptions,
             remote_topics: remote_topics
         }}

      {:error, error} ->
        send(caller, {ref, {:error, error}})
        {:noreply, state}
    end
  end

  def handle_info({:channel_ex_cancel_subscription, sub_id}, state) do
    case Map.pop(state.subscriptions, sub_id) do
      {nil, _subscriptions} ->
        {:noreply, state}

      {topic, subscriptions} ->
        topic_callbacks =
          state.callbacks
          |> Map.get(topic, %{})
          |> Map.delete(sub_id)

        callbacks =
          if map_size(topic_callbacks) == 0 do
            Map.delete(state.callbacks, topic)
          else
            Map.put(state.callbacks, topic, topic_callbacks)
          end

        remote_topics =
          if Map.has_key?(callbacks, topic) do
            state.remote_topics
          else
            ChannelEx.Core.remote_topics_remove(state.remote_topics, topic)
          end

        {:noreply,
         %{
           state
           | subscriptions: subscriptions,
             callbacks: callbacks,
             remote_topics: remote_topics
         }}
    end
  end

  def handle_info({:channel_ex_subscription_event, topic, body}, state) do
    if MapSet.member?(state.remote_topics, topic) do
      ChannelEx.Core.dispatch_local_event(state.callbacks, topic, body)
    end

    {:noreply, state}
  end

  def handle_info({:channel_ex_cancel_request, _ref}, state), do: {:noreply, state}

  def handle_info(:channel_ex_stop, state) do
    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    ChannelEx.Core.cleanup_publishers(state.publisher_refs)
    ChannelEx.Core.run_disconnect(state.channel, state.state_ref, state.sender)
    ChannelEx.ConnectionState.stop(state.state_ref)
    :ok
  end

  defp response_map({:ok, value}), do: %{body: value}
  defp response_map({:error, error}), do: %{error: error}
end
