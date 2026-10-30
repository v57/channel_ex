if Code.ensure_loaded?(:quicer) do
  defmodule ChannelEx.Quic.Owner do
    @moduledoc false
    # The Sender messages a connection process handles (requests, streams, subscriptions,
    # cancels and local events), shared by the QUIC client and server connections. `drain/2`
    # takes every such message already waiting, so they go out in one batched frame.

    alias ChannelEx.Peer

    def apply_message(peer, {:channel_ex_request, caller, ref, path, body, context}) do
      {peer, outgoing} = Peer.queue_request(peer, caller, ref, path, body, context)
      {:ok, peer, outgoing}
    end

    def apply_message(peer, {:channel_ex_stream_request, caller, ref, path, body, context}) do
      {peer, outgoing} = Peer.queue_stream_request(peer, caller, ref, path, body, context)
      {:ok, peer, outgoing}
    end

    def apply_message(peer, {:channel_ex_subscribe, caller, ref, path, body, context, event_fun}) do
      {peer, outgoing} =
        Peer.queue_subscribe_request(peer, caller, ref, path, body, context, event_fun)

      {:ok, peer, outgoing}
    end

    def apply_message(peer, {:channel_ex_cancel_request, ref}) do
      {peer, outgoing} = Peer.cancel_request(peer, ref)
      {:ok, peer, outgoing}
    end

    def apply_message(peer, {:channel_ex_cancel_subscription, sub_id}) do
      {peer, outgoing} = Peer.cancel_subscription(peer, sub_id)
      {:ok, peer, outgoing}
    end

    def apply_message(peer, {:channel_ex_local_publish, topic, body}) do
      {peer, outgoing} = Peer.local_subscription_event(peer, topic, body)
      {:ok, peer, outgoing}
    end

    def apply_message(_peer, _message), do: :unknown

    @doc "Applies the waiting owner messages; `outgoing` and the result are in reverse order."
    def drain(peer, outgoing) do
      receive do
        message
        when is_tuple(message) and
               elem(message, 0) in [
                 :channel_ex_request,
                 :channel_ex_stream_request,
                 :channel_ex_subscribe,
                 :channel_ex_cancel_request,
                 :channel_ex_cancel_subscription,
                 :channel_ex_local_publish
               ] ->
          case apply_message(peer, message) do
            {:ok, peer, next} -> drain(peer, Enum.reverse(next, outgoing))
            :unknown -> {peer, outgoing}
          end
      after
        0 -> {peer, outgoing}
      end
    end

    def fail_pending(pending) do
      Enum.each(pending, fn
        {_id, %{kind: :subscribe, caller: caller, ref: ref}} ->
          send(caller, {ref, {:error, "disconnected"}})

        {_id, %{caller: caller, ref: ref}} ->
          send(caller, {ref, %{error: "disconnected"}})
      end)
    end
  end
end
