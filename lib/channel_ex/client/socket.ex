defmodule ChannelEx.Client.Socket do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def sender(pid), do: GenServer.call(pid, :sender)
  def status(pid), do: GenServer.call(pid, :status)
  def stop(pid), do: send(pid, :channel_ex_stop)
  def throttle(_pid), do: :ok

  @impl true
  def init(opts) do
    uri = opts |> Keyword.fetch!(:url) |> URI.parse()
    host = uri.host || "127.0.0.1"
    port = uri.port || 80
    path = ws_path(uri)
    headers = ws_headers(Keyword.get(opts, :headers))

    {:ok, socket} =
      :gen_tcp.connect(String.to_charlist(host), port, [
        :binary,
        {:packet, :raw},
        {:active, false},
        {:nodelay, true},
        {:reuseaddr, true}
      ])

    {request, key} = ChannelEx.WebSocket.client_handshake_request(host, port, path, headers)
    :ok = :gen_tcp.send(socket, request)
    :ok = recv_handshake!(socket, key, <<>>)
    :ok = :inet.setopts(socket, active: :once)

    channel = Keyword.fetch!(opts, :channel)
    {:ok, state_ref} = ChannelEx.ConnectionState.start_link(Keyword.fetch!(opts, :state_ref))
    owner = self()
    peer = ChannelEx.Peer.new(channel, owner, :client, state_ref)

    publisher_refs =
      ChannelEx.Core.register_publishers(channel, fn event ->
        send(owner, {:channel_ex_local_publish, event.topic, event.body})
      end)

    run_on_connect(Keyword.get(opts, :on_connect), peer.sender)

    {:ok,
     %{
       socket: socket,
       connected: true,
       recv_buffer: <<>>,
       peer: peer,
       publisher_refs: publisher_refs
     }}
  end

  @impl true
  def handle_call(:sender, _from, state), do: {:reply, state.peer.sender, state}

  def handle_call(:status, _from, state) do
    {:reply,
     %{
       connected: state.connected,
       pending: ChannelEx.Peer.pending_count(state.peer),
       gun_pid: nil,
       ws_ref: nil
     }, state}
  end

  @impl true
  def handle_info({:channel_ex_request, caller, ref, path, body, context}, state) do
    {peer, outgoing} = ChannelEx.Peer.queue_request(state.peer, caller, ref, path, body, context)
    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:channel_ex_stream_request, caller, ref, path, body, context}, state) do
    {peer, outgoing} =
      ChannelEx.Peer.queue_stream_request(state.peer, caller, ref, path, body, context)

    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:channel_ex_subscribe, caller, ref, path, body, context, event_fun}, state) do
    {peer, outgoing} =
      ChannelEx.Peer.queue_subscribe_request(
        state.peer,
        caller,
        ref,
        path,
        body,
        context,
        event_fun
      )

    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:channel_ex_cancel_request, ref}, state) do
    {peer, outgoing} = ChannelEx.Peer.cancel_request(state.peer, ref)
    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:channel_ex_cancel_subscription, sub_id}, state) do
    {peer, outgoing} = ChannelEx.Peer.cancel_subscription(state.peer, sub_id)
    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:channel_ex_local_publish, topic, body}, state) do
    {peer, outgoing} = ChannelEx.Peer.local_subscription_event(state.peer, topic, body)
    {:noreply, drain_peer_outgoing(%{state | peer: peer}, outgoing)}
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    state =
      state
      |> Map.update!(:recv_buffer, &(&1 <> data))
      |> process_recv_buffer()

    if state.connected do
      :ok = :inet.setopts(socket, active: :once)
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state) do
    {:stop, :normal, %{state | connected: false}}
  end

  def handle_info({:tcp_error, socket, _reason}, %{socket: socket} = state) do
    {:stop, :normal, %{state | connected: false}}
  end

  def handle_info(:channel_ex_stop, state) do
    maybe_send_close(state)
    {:stop, :normal, %{state | connected: false}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    fail_pending(state.peer.pending)
    ChannelEx.Core.cleanup_publishers(state.publisher_refs)
    ChannelEx.Core.run_disconnect(state.peer.channel, state.peer.state_ref, state.peer.sender)
    ChannelEx.ConnectionState.stop(state.peer.state_ref)

    if state.socket do
      :gen_tcp.close(state.socket)
    end

    :ok
  end

  defp drain_peer_outgoing(state, outgoing) do
    {state, outgoing} = drain_owner_messages(state, Enum.reverse(outgoing))
    send_peer_outgoing(state, Enum.reverse(outgoing))
  end

  defp drain_owner_messages(state, outgoing) do
    receive do
      {:channel_ex_request, caller, ref, path, body, context} ->
        {peer, next_outgoing} =
          ChannelEx.Peer.queue_request(state.peer, caller, ref, path, body, context)

        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))

      {:channel_ex_stream_request, caller, ref, path, body, context} ->
        {peer, next_outgoing} =
          ChannelEx.Peer.queue_stream_request(state.peer, caller, ref, path, body, context)

        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))

      {:channel_ex_subscribe, caller, ref, path, body, context, event_fun} ->
        {peer, next_outgoing} =
          ChannelEx.Peer.queue_subscribe_request(
            state.peer,
            caller,
            ref,
            path,
            body,
            context,
            event_fun
          )

        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))

      {:channel_ex_cancel_request, ref} ->
        {peer, next_outgoing} = ChannelEx.Peer.cancel_request(state.peer, ref)
        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))

      {:channel_ex_cancel_subscription, sub_id} ->
        {peer, next_outgoing} = ChannelEx.Peer.cancel_subscription(state.peer, sub_id)
        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))

      {:channel_ex_local_publish, topic, body} ->
        {peer, next_outgoing} = ChannelEx.Peer.local_subscription_event(state.peer, topic, body)
        drain_owner_messages(%{state | peer: peer}, Enum.reverse(next_outgoing, outgoing))
    after
      0 -> {state, outgoing}
    end
  end

  defp send_peer_outgoing(%{connected: true, socket: socket} = state, [_ | _] = outgoing) do
    payload =
      outgoing
      |> ChannelEx.Wire.encode!()
      |> IO.iodata_to_binary()
      |> then(&ChannelEx.WebSocket.encode_text(&1, true))

    :ok = :gen_tcp.send(socket, payload)
    state
  end

  defp send_peer_outgoing(state, _outgoing), do: state

  defp process_recv_buffer(state) do
    case ChannelEx.WebSocket.decode_frames(state.recv_buffer) do
      {:ok, frames, rest} ->
        Enum.reduce(frames, %{state | recv_buffer: rest}, fn frame, acc ->
          handle_frame(acc, frame)
        end)

      :more ->
        state

      {:error, _reason} ->
        %{state | connected: false}
    end
  end

  defp handle_frame(state, {:text, payload}) do
    messages = ChannelEx.Wire.decode_any(payload)
    {peer, outgoing} = ChannelEx.Peer.receive_messages(state.peer, messages)
    send_peer_outgoing(%{state | peer: peer}, outgoing)
  end

  defp handle_frame(state, {:ping, payload}) do
    encoded = ChannelEx.WebSocket.encode_pong(payload, true)
    :ok = :gen_tcp.send(state.socket, encoded)
    state
  end

  defp handle_frame(state, {:pong, _payload}), do: state
  defp handle_frame(state, {:close, _payload}), do: %{state | connected: false}

  defp maybe_send_close(%{connected: true, socket: socket}) do
    _ = :gen_tcp.send(socket, ChannelEx.WebSocket.encode_close(<<>>, true))
    :ok
  end

  defp maybe_send_close(_state), do: :ok

  defp ws_path(uri) do
    path = if uri.path in [nil, ""], do: "/", else: uri.path

    case uri.query do
      nil -> path
      "" -> path
      query -> path <> "?" <> query
    end
  end

  defp ws_headers(nil), do: []

  defp ws_headers(fun) when is_function(fun, 0) do
    headers = fun.() || %{}

    if is_map(headers) do
      Enum.reduce(headers, [], fn {key, value}, acc ->
        if is_binary(value) and byte_size(value) > 0 do
          [{to_string(key), value} | acc]
        else
          acc
        end
      end)
    else
      []
    end
  rescue
    _ -> []
  end

  defp recv_handshake!(socket, key, buffer) do
    case ChannelEx.WebSocket.parse_http_response(buffer, key) do
      {:ok, _headers, _rest} ->
        :ok

      :more ->
        {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)
        recv_handshake!(socket, key, buffer <> data)

      {:error, reason} ->
        raise "websocket handshake failed: #{inspect(reason)}"
    end
  end

  defp run_on_connect(nil, _sender), do: :ok

  defp run_on_connect(fun, sender) when is_function(fun, 1) do
    try do
      fun.(sender)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp fail_pending(pending) do
    Enum.each(pending, fn
      {_id, %{kind: :subscribe, caller: caller, ref: ref}} ->
        send(caller, {ref, {:error, "disconnected"}})

      {_id, %{caller: caller, ref: ref}} ->
        send(caller, {ref, %{error: "disconnected"}})
    end)
  end
end
