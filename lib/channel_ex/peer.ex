defmodule ChannelEx.Peer do
  @moduledoc false

  alias ChannelEx.Message

  defstruct [
    :channel,
    :owner,
    :sender,
    :state_ref,
    next_wire_id: 0,
    next_sub_id: 0,
    pending: %{},
    subscriptions: %{},
    callbacks: %{},
    remote_topics: MapSet.new()
  ]

  def new(channel, owner, transport, state_ref) do
    sender = ChannelEx.Sender.new(channel, owner, transport, state_ref)
    %__MODULE__{channel: channel, owner: owner, sender: sender, state_ref: state_ref}
  end

  def queue_request(peer, caller, ref, path, body, context) do
    wire_id = peer.next_wire_id
    pending = Map.put(peer.pending, wire_id, %{kind: :request, caller: caller, ref: ref})

    {%{peer | next_wire_id: wire_id + 1, pending: pending},
     [%{id: wire_id, path: path, body: body, context: context}]}
  end

  def queue_stream_request(peer, caller, ref, path, body, context) do
    wire_id = peer.next_wire_id
    pending = Map.put(peer.pending, wire_id, %{kind: :stream, caller: caller, ref: ref})

    {%{peer | next_wire_id: wire_id + 1, pending: pending},
     [%{id: wire_id, stream: path, body: body, context: context}]}
  end

  def queue_subscribe_request(peer, caller, ref, path, body, context, event_fun) do
    wire_id = peer.next_wire_id

    pending =
      Map.put(peer.pending, wire_id, %{
        kind: :subscribe,
        caller: caller,
        ref: ref,
        event_fun: event_fun
      })

    {%{peer | next_wire_id: wire_id + 1, pending: pending},
     [%{id: wire_id, sub: path, body: body, context: context}]}
  end

  def cancel_request(peer, ref) do
    case Enum.find(peer.pending, fn {_id, entry} -> entry.ref == ref end) do
      nil ->
        {peer, []}

      {wire_id, _entry} ->
        {%{peer | pending: Map.delete(peer.pending, wire_id)}, [%{cancel: wire_id}]}
    end
  end

  def cancel_subscription(peer, sub_id) do
    case Map.pop(peer.subscriptions, sub_id) do
      {nil, _subscriptions} ->
        {peer, []}

      {topic, subscriptions} ->
        topic_callbacks =
          peer.callbacks
          |> Map.get(topic, %{})
          |> Map.delete(sub_id)

        callbacks =
          if map_size(topic_callbacks) == 0 do
            Map.delete(peer.callbacks, topic)
          else
            Map.put(peer.callbacks, topic, topic_callbacks)
          end

        {remote_topics, outgoing} =
          if Map.has_key?(callbacks, topic) do
            {peer.remote_topics, []}
          else
            {MapSet.delete(peer.remote_topics, topic), [%{unsub: topic}]}
          end

        {%{
           peer
           | subscriptions: subscriptions,
             callbacks: callbacks,
             remote_topics: remote_topics
         }, outgoing}
    end
  end

  def receive_messages(peer, payloads) when is_list(payloads) do
    {peer, outgoing} =
      Enum.reduce(payloads, {peer, []}, fn payload, {peer_acc, outgoing_acc} ->
        {next_peer, outgoing} = receive_message(peer_acc, payload)
        {next_peer, Enum.reverse(outgoing, outgoing_acc)}
      end)

    {peer, Enum.reverse(outgoing)}
  end

  def receive_message(peer, payload) when is_map(payload) do
    cond do
      is_binary(Message.get(payload, :path)) ->
        handle_post(peer, payload)

      is_binary(Message.get(payload, :stream)) ->
        handle_stream(peer, payload)

      is_integer(Message.get(payload, :cancel)) ->
        {peer, []}

      is_binary(Message.get(payload, :sub)) ->
        handle_subscribe(peer, payload)

      is_binary(Message.get(payload, :unsub)) ->
        topic = Message.get(payload, :unsub)
        {%{peer | remote_topics: MapSet.delete(peer.remote_topics, topic)}, []}

      is_binary(Message.get(payload, :topic)) and is_integer(Message.get(payload, :id)) ->
        handle_subscribe_reply_or_topic(peer, payload)

      is_integer(Message.get(payload, :id)) ->
        handle_response(peer, payload)

      is_binary(Message.get(payload, :topic)) ->
        ChannelEx.Core.dispatch_local_event(
          peer.callbacks,
          Message.get(payload, :topic),
          Message.get(payload, :body)
        )

        {peer, []}

      true ->
        {peer, []}
    end
  end

  def local_subscription_event(peer, topic, body) do
    if MapSet.member?(peer.remote_topics, topic) and not is_nil(body) do
      {peer, [%{topic: topic, body: body}]}
    else
      {peer, []}
    end
  end

  def pending_count(peer), do: map_size(peer.pending)

  defp handle_post(peer, payload) do
    path = Message.get(payload, :path)
    id = Message.get(payload, :id)

    response =
      ChannelEx.Core.invoke_post(
        peer.channel,
        peer.sender,
        peer.state_ref,
        path,
        Message.get(payload, :body),
        Message.get(payload, :context)
      )

    if is_integer(id) do
      {peer, [Map.put(response_map(response), :id, id)]}
    else
      {peer, []}
    end
  end

  defp handle_stream(peer, payload) do
    path = Message.get(payload, :stream)
    id = Message.get(payload, :id)

    if not is_integer(id) do
      {peer, []}
    else
      case ChannelEx.Core.invoke_stream(
             peer.channel,
             peer.sender,
             peer.state_ref,
             path,
             Message.get(payload, :body),
             Message.get(payload, :context)
           ) do
        {:ok, values} ->
          responses =
            Enum.map(values, fn value -> %{id: id, body: value} end) ++
              [%{id: id, done: true}]

          {peer, responses}

        {:error, error} ->
          {peer, [%{id: id, error: error}]}
      end
    end
  end

  defp handle_subscribe(peer, payload) do
    id = Message.get(payload, :id)

    if not is_integer(id) do
      {peer, []}
    else
      case ChannelEx.Core.invoke_subscribe(
             peer.channel,
             Message.get(payload, :sub),
             Message.get(payload, :body)
           ) do
        {:ok, topic, body} ->
          remote_topics = MapSet.put(peer.remote_topics, topic)
          {%{peer | remote_topics: remote_topics}, [%{id: id, topic: topic, body: body}]}

        {:error, error} ->
          {peer, [%{id: id, error: error}]}
      end
    end
  end

  defp handle_subscribe_reply_or_topic(peer, payload) do
    id = Message.get(payload, :id)

    case Map.pop(peer.pending, id) do
      {%{kind: :subscribe, caller: caller, ref: ref, event_fun: event_fun}, pending} ->
        case Message.get(payload, :error) do
          nil ->
            topic = Message.get(payload, :topic)
            sub_id = peer.next_sub_id + 1
            callbacks = put_callback(peer.callbacks, topic, sub_id, event_fun)
            subscriptions = Map.put(peer.subscriptions, sub_id, topic)
            send(caller, {ref, {:ok, sub_id}})

            {%{
               peer
               | pending: pending,
                 next_sub_id: sub_id,
                 callbacks: callbacks,
                 subscriptions: subscriptions
             }, []}

          error ->
            send(caller, {ref, {:error, error}})
            {%{peer | pending: pending}, []}
        end

      {nil, _pending} ->
        ChannelEx.Core.dispatch_local_event(
          peer.callbacks,
          Message.get(payload, :topic),
          Message.get(payload, :body)
        )

        {peer, []}

      {_other, _pending} ->
        handle_response(peer, payload)
    end
  end

  defp handle_response(peer, payload) do
    id = Message.get(payload, :id)

    case Map.get(peer.pending, id) do
      %{kind: :request, caller: caller, ref: ref} ->
        send(caller, {ref, payload})
        {%{peer | pending: Map.delete(peer.pending, id)}, []}

      %{kind: :stream, caller: caller, ref: ref} ->
        send(caller, {ref, payload})

        if Message.get(payload, :done, false) or Message.get(payload, :error) do
          {%{peer | pending: Map.delete(peer.pending, id)}, []}
        else
          {peer, []}
        end

      %{kind: :subscribe} ->
        handle_subscribe_reply_or_topic(peer, payload)

      nil ->
        {peer, []}
    end
  end

  defp response_map({:ok, value}), do: %{body: value}
  defp response_map({:error, error}), do: %{error: error}

  defp put_callback(callbacks, topic, sub_id, event_fun) do
    Map.update(callbacks, topic, %{sub_id => event_fun}, fn topic_callbacks ->
      Map.put(topic_callbacks, sub_id, event_fun)
    end)
  end
end
