defmodule ChannelEx.ServerConnection do
  @moduledoc false
  # One process per accepted socket, started under the listener's DynamicSupervisor
  # (restart: :temporary). A hostile message, a failing handler or a write to a peer
  # that already disconnected can only take down this connection, never the listener
  # or other clients. The connection's Sender has this process as its owner, so
  # server-initiated requests (`sender.send/3` to the client) are routed here.

  use GenServer, restart: :temporary

  alias ChannelEx.WebSocket

  def start_link({_channel, _options, _listener} = args) do
    GenServer.start_link(__MODULE__, args)
  end

  @doc "Hands the accepted socket to the connection process (call after controlling_process/2)."
  def attach(pid, socket), do: send(pid, {:channel_ex_attach, socket})

  @impl true
  def init({channel, options, listener}) do
    # Trap exits so the supervisor's shutdown runs terminate/2 (on_disconnect callbacks).
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       channel: channel,
       options: options,
       listener: listener,
       max_payload: Keyword.get(options, :max_payload, WebSocket.default_max_payload()),
       socket: nil,
       phase: :handshake,
       recv_buffer: <<>>,
       fragments: nil,
       peer: nil
     }}
  end

  @impl true
  def handle_info({:channel_ex_attach, socket}, state) do
    state = %{state | socket: socket}

    case :inet.setopts(socket, active: :once) do
      :ok -> {:noreply, state}
      {:error, _reason} -> {:stop, :normal, state}
    end
  end

  def handle_info({:tcp, socket, data}, %{socket: socket} = state) do
    case process_buffer(%{state | recv_buffer: state.recv_buffer <> data}) do
      {:ok, state} ->
        case :inet.setopts(socket, active: :once) do
          :ok -> {:noreply, state}
          {:error, _reason} -> {:stop, :normal, state}
        end

      {:close, state} ->
        {:stop, :normal, state}
    end
  end

  def handle_info({:tcp_closed, socket}, %{socket: socket} = state), do: {:stop, :normal, state}

  def handle_info({:tcp_error, socket, _reason}, %{socket: socket} = state),
    do: {:stop, :normal, state}

  def handle_info({:channel_ex_local_publish, topic, body}, %{peer: peer} = state)
      when not is_nil(peer) do
    {peer, outgoing} = ChannelEx.Peer.local_subscription_event(peer, topic, body)
    send_outgoing(%{state | peer: peer}, outgoing)
  end

  def handle_info({:channel_ex_request, caller, ref, path, body, context}, %{peer: peer} = state)
      when not is_nil(peer) do
    {peer, outgoing} = ChannelEx.Peer.queue_request(peer, caller, ref, path, body, context)
    send_outgoing(%{state | peer: peer}, outgoing)
  end

  def handle_info(
        {:channel_ex_stream_request, caller, ref, path, body, context},
        %{peer: peer} = state
      )
      when not is_nil(peer) do
    {peer, outgoing} = ChannelEx.Peer.queue_stream_request(peer, caller, ref, path, body, context)
    send_outgoing(%{state | peer: peer}, outgoing)
  end

  def handle_info(
        {:channel_ex_subscribe, caller, ref, path, body, context, event_fun},
        %{peer: peer} = state
      )
      when not is_nil(peer) do
    {peer, outgoing} =
      ChannelEx.Peer.queue_subscribe_request(peer, caller, ref, path, body, context, event_fun)

    send_outgoing(%{state | peer: peer}, outgoing)
  end

  def handle_info({:channel_ex_cancel_request, ref}, %{peer: peer} = state)
      when not is_nil(peer) do
    {peer, outgoing} = ChannelEx.Peer.cancel_request(peer, ref)
    send_outgoing(%{state | peer: peer}, outgoing)
  end

  def handle_info({:channel_ex_cancel_subscription, sub_id}, %{peer: peer} = state)
      when not is_nil(peer) do
    {peer, outgoing} = ChannelEx.Peer.cancel_subscription(peer, sub_id)
    send_outgoing(%{state | peer: peer}, outgoing)
  end

  # Sender.stop/1 on a server-side sender closes this connection only.
  def handle_info(:channel_ex_stop, state) do
    if state.socket && state.peer do
      _ = :gen_tcp.send(state.socket, WebSocket.encode_close(<<>>, false))
    end

    {:stop, :normal, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if peer = state.peer do
      fail_pending(peer.pending)

      run_callback(
        Keyword.get(state.options, :on_disconnect),
        peer.state_ref,
        peer.sender
      )

      ChannelEx.Core.run_disconnect(state.channel, peer.state_ref, peer.sender)
      ChannelEx.ConnectionState.stop(peer.state_ref)
    end

    if state.socket, do: :gen_tcp.close(state.socket)
    :ok
  end

  defp process_buffer(%{phase: :handshake} = state) do
    case WebSocket.parse_http_request(state.recv_buffer) do
      {:ok, request, rest} ->
        with {:ok, response} <- WebSocket.server_handshake_response(request.headers),
             :ok <- :gen_tcp.send(state.socket, response) do
          state_value = build_state(state.options, request)
          {:ok, state_ref} = ChannelEx.ConnectionState.start_link(state_value)
          peer = ChannelEx.Peer.new(state.channel, self(), {:server, state.socket}, state_ref)
          run_callback(Keyword.get(state.options, :on_connect), state_ref, peer.sender)
          process_buffer(%{state | phase: :active, recv_buffer: rest, peer: peer})
        else
          _ -> {:close, state}
        end

      :more ->
        {:ok, state}

      {:error, _reason} ->
        {:close, state}
    end
  end

  defp process_buffer(%{phase: :active} = state) do
    with {:ok, frames, rest} <- WebSocket.decode_frames(state.recv_buffer, state.max_payload),
         {:ok, frames, fragments} <-
           WebSocket.reassemble(frames, state.fragments, state.max_payload) do
      handle_frames(frames, %{state | recv_buffer: rest, fragments: fragments})
    else
      :more -> {:ok, state}
      {:error, _reason} -> {:close, state}
    end
  end

  defp handle_frames([], state), do: {:ok, state}

  defp handle_frames([frame | rest], state) do
    case handle_frame(frame, state) do
      {:noreply, state} -> handle_frames(rest, state)
      {:stop, _reason, state} -> {:close, state}
    end
  end

  defp handle_frame({:text, payload}, state) do
    case ChannelEx.Wire.decode_messages(payload) do
      {:ok, messages} ->
        {peer, outgoing} = ChannelEx.Peer.receive_messages(state.peer, messages)
        send_outgoing(%{state | peer: peer}, outgoing)

      # TS parity: an unparseable message is dropped and the connection stays open.
      :error ->
        {:noreply, state}
    end
  end

  # TS parity: the reference server ignores binary messages.
  defp handle_frame({:binary, _payload}, state), do: {:noreply, state}

  defp handle_frame({:ping, payload}, state) do
    case :gen_tcp.send(state.socket, WebSocket.encode_pong(payload, false)) do
      :ok -> {:noreply, state}
      {:error, _reason} -> {:stop, :normal, state}
    end
  end

  defp handle_frame({:pong, _payload}, state), do: {:noreply, state}
  defp handle_frame({:close, _payload}, state), do: {:stop, :normal, state}

  defp send_outgoing(state, []), do: {:noreply, state}

  defp send_outgoing(state, outgoing) do
    payload =
      outgoing
      |> ChannelEx.Wire.encode!()
      |> IO.iodata_to_binary()
      |> WebSocket.encode_text(false)

    case :gen_tcp.send(state.socket, payload) do
      :ok -> {:noreply, state}
      {:error, _reason} -> {:stop, :normal, state}
    end
  end

  defp build_state(options, request) do
    # Real headers win over query parameters (TS server.ts parity).
    merged = Map.merge(request.query, request.headers)

    case Keyword.get(options, :state) do
      fun when is_function(fun, 1) -> fun.(merged)
      nil -> %{}
      other -> other
    end
  end

  defp run_callback(nil, _state_ref, _sender), do: :ok

  defp run_callback(fun, state_ref, sender) when is_function(fun, 1) do
    try do
      fun.(%{state: state_ref, sender: sender})
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
