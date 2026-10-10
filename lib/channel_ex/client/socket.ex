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

  @doc """
  Opens the TCP connection and performs the websocket handshake in the calling process.
  Returns `{:ok, socket, rest}` (bytes received after the 101 reply) or `{:error, reason}`;
  never raises on network errors, a non-101 reply or a silent server.
  """
  def open(opts) do
    uri = opts |> Keyword.fetch!(:url) |> URI.parse()
    host = uri.host || "127.0.0.1"
    port = uri.port || 80
    path = ws_path(uri)
    headers = ws_headers(Keyword.get(opts, :headers))
    timeout = Keyword.get(opts, :connect_timeout, 5_000)

    case :gen_tcp.connect(
           String.to_charlist(host),
           port,
           [:binary, {:packet, :raw}, {:active, false}, {:nodelay, true}, {:reuseaddr, true}],
           timeout
         ) do
      {:ok, socket} ->
        {request, key} = ChannelEx.WebSocket.client_handshake_request(host, port, path, headers)
        deadline = System.monotonic_time(:millisecond) + timeout

        result =
          case :gen_tcp.send(socket, request) do
            :ok -> recv_handshake(socket, key, <<>>, deadline)
            {:error, _reason} = error -> error
          end

        case result do
          {:ok, rest} ->
            {:ok, socket, rest}

          {:error, _reason} = error ->
            :gen_tcp.close(socket)
            error
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Makes `pid` the socket's controlling process and starts reading."
  def attach(pid, socket) do
    with :ok <- :gen_tcp.controlling_process(socket, pid) do
      send(pid, {:channel_ex_attach, socket})
      :ok
    end
  end

  @impl true
  def init(opts) do
    case Keyword.fetch(opts, :socket) do
      {:ok, socket} ->
        {:ok, start_peer(opts, socket, Keyword.get(opts, :recv_buffer, <<>>))}

      :error ->
        # Standalone start_link(url: ...): connect here, and stop cleanly on failure.
        case open(opts) do
          {:ok, socket, rest} ->
            state = start_peer(opts, socket, <<>>)
            send(self(), {:channel_ex_attach, socket})
            {:ok, %{state | recv_buffer: rest}}

          {:error, reason} ->
            {:stop, {:shutdown, reason}}
        end
    end
  end

  defp start_peer(opts, socket, recv_buffer) do
    channel = Keyword.fetch!(opts, :channel)
    {:ok, state_ref} = ChannelEx.ConnectionState.start_link(Keyword.fetch!(opts, :state_ref))
    owner = self()
    peer = ChannelEx.Peer.new(channel, owner, :client, state_ref)

    publisher_refs =
      ChannelEx.Core.register_publishers(channel, fn event ->
        send(owner, {:channel_ex_local_publish, event.topic, event.body})
      end)

    run_on_connect(Keyword.get(opts, :on_connect), peer.sender)

    %{
      socket: socket,
      connected: true,
      recv_buffer: recv_buffer,
      fragments: nil,
      max_payload: Keyword.get(opts, :max_payload, ChannelEx.WebSocket.default_max_payload()),
      peer: peer,
      publisher_refs: publisher_refs
    }
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

  def handle_info({:channel_ex_attach, socket}, %{socket: socket} = state) do
    # Bytes that arrived together with the 101 reply are processed first.
    state = if state.recv_buffer == <<>>, do: state, else: process_recv_buffer(state)

    with true <- state.connected,
         :ok <- :inet.setopts(socket, active: :once) do
      {:noreply, state}
    else
      _ -> {:stop, :normal, %{state | connected: false}}
    end
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    state =
      state
      |> Map.update!(:recv_buffer, &(&1 <> data))
      |> process_recv_buffer()

    with true <- state.connected,
         :ok <- :inet.setopts(socket, active: :once) do
      {:noreply, state}
    else
      _ -> {:stop, :normal, %{state | connected: false}}
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

    case :gen_tcp.send(socket, payload) do
      :ok -> state
      {:error, _reason} -> %{state | connected: false}
    end
  end

  defp send_peer_outgoing(state, _outgoing), do: state

  defp process_recv_buffer(state) do
    with {:ok, frames, rest} <-
           ChannelEx.WebSocket.decode_frames(state.recv_buffer, state.max_payload),
         {:ok, frames, fragments} <-
           ChannelEx.WebSocket.reassemble(frames, state.fragments, state.max_payload) do
      Enum.reduce(frames, %{state | recv_buffer: rest, fragments: fragments}, fn frame, acc ->
        if acc.connected, do: handle_frame(acc, frame), else: acc
      end)
    else
      :more -> state
      {:error, _reason} -> %{state | connected: false}
    end
  end

  # Binary replies carry the same JSON as text ones (the TS client would drop them and
  # leave the call pending forever).
  defp handle_frame(state, {kind, payload}) when kind in [:text, :binary] do
    case ChannelEx.Wire.decode_messages(payload) do
      {:ok, messages} ->
        {peer, outgoing} = ChannelEx.Peer.receive_messages(state.peer, messages)
        send_peer_outgoing(%{state | peer: peer}, outgoing)

      # An undecodable reply can't be matched to its request: close the connection so
      # every pending call settles with "disconnected" instead of hanging.
      :error ->
        %{state | connected: false}
    end
  end

  defp handle_frame(state, {:ping, payload}) do
    case :gen_tcp.send(state.socket, ChannelEx.WebSocket.encode_pong(payload, true)) do
      :ok -> state
      {:error, _reason} -> %{state | connected: false}
    end
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

  defp recv_handshake(socket, key, buffer, deadline) do
    case ChannelEx.WebSocket.parse_http_response(buffer, key) do
      {:ok, _headers, rest} ->
        {:ok, rest}

      :more ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 0)

        case :gen_tcp.recv(socket, 0, remaining) do
          {:ok, data} -> recv_handshake(socket, key, buffer <> data, deadline)
          {:error, reason} -> {:error, {:handshake_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:handshake_failed, reason}}
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
