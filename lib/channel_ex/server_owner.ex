defmodule ChannelEx.ServerOwner do
  @moduledoc false

  use GenServer

  def start_link(channel, address, options) do
    GenServer.start_link(__MODULE__, {channel, address, options})
  end

  def port(pid), do: GenServer.call(pid, :port)

  @impl true
  def init({channel, address, options}) do
    {ip, port} = parse_address(address)

    {:ok, listener} =
      :gen_tcp.listen(port, [
        :binary,
        {:packet, :raw},
        {:active, false},
        {:reuseaddr, true},
        {:nodelay, true},
        {:ip, ip}
      ])

    actual_port = listener_port(listener)
    owner = self()

    acceptor =
      spawn_link(fn ->
        accept_loop(listener, owner)
      end)

    publisher_refs =
      ChannelEx.Core.register_publishers(channel, fn event ->
        send(owner, {:channel_ex_local_publish, event.topic, event.body})
      end)

    {:ok,
     %{
       channel: channel,
       listener: listener,
       port: actual_port,
       acceptor: acceptor,
       options: options,
       connections: %{},
       publisher_refs: publisher_refs,
       stopping: false
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def handle_info({:accepted, socket}, state) do
    :ok = :inet.setopts(socket, active: :once)

    connection = %{
      phase: :handshake,
      recv_buffer: <<>>
    }

    {:noreply, %{state | connections: Map.put(state.connections, socket, connection)}}
  end

  def handle_info({:accept_error, _reason}, state), do: {:noreply, state}

  def handle_info({:tcp, socket, data}, state) do
    state =
      state
      |> update_in([:connections, socket, :recv_buffer], &((&1 || <<>>) <> data))
      |> process_socket_buffer(socket)

    if Map.has_key?(state.connections, socket) do
      :ok = :inet.setopts(socket, active: :once)
    end

    {:noreply, state}
  end

  def handle_info({:tcp_closed, socket}, state) do
    {:noreply, remove_connection(state, socket)}
  end

  def handle_info({:tcp_error, socket, _reason}, state) do
    {:noreply, remove_connection(state, socket)}
  end

  def handle_info({:channel_ex_local_publish, topic, body}, state) do
    state =
      Enum.reduce(Map.keys(state.connections), state, fn socket, acc ->
        case get_in(acc.connections, [socket, :peer]) do
          nil ->
            acc

          peer ->
            {peer, outgoing} = ChannelEx.Peer.local_subscription_event(peer, topic, body)

            acc
            |> put_in([:connections, socket, :peer], peer)
            |> send_socket_outgoing(socket, outgoing)
        end
      end)

    {:noreply, state}
  end

  def handle_info(:channel_ex_stop, state) do
    {:stop, :normal, %{state | stopping: true}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.listener, do: :gen_tcp.close(state.listener)
    if is_pid(state.acceptor), do: Process.exit(state.acceptor, :normal)

    state =
      Enum.reduce(Map.keys(state.connections), state, fn socket, acc ->
        remove_connection(acc, socket)
      end)

    ChannelEx.Core.cleanup_publishers(state.publisher_refs)
    :ok
  end

  defp process_socket_buffer(state, socket) do
    connection = Map.fetch!(state.connections, socket)

    case connection.phase do
      :handshake ->
        case ChannelEx.WebSocket.parse_http_request(connection.recv_buffer) do
          {:ok, request, rest} ->
            case ChannelEx.WebSocket.server_handshake_response(request.headers) do
              {:ok, response} ->
                :ok = :gen_tcp.send(socket, response)
                state_value = build_state(state.options, request)
                {:ok, state_ref} = ChannelEx.ConnectionState.start_link(state_value)
                peer = ChannelEx.Peer.new(state.channel, self(), {:server, socket}, state_ref)

                run_server_callback(
                  Keyword.get(state.options, :on_connect),
                  state_ref,
                  peer.sender
                )

                state
                |> put_in(
                  [:connections, socket],
                  Map.merge(connection, %{phase: :active, recv_buffer: rest, peer: peer})
                )
                |> process_socket_buffer(socket)

              {:error, _reason} ->
                remove_connection(state, socket)
            end

          :more ->
            state

          {:error, _reason} ->
            remove_connection(state, socket)
        end

      :active ->
        case ChannelEx.WebSocket.decode_frames(connection.recv_buffer) do
          {:ok, frames, rest} ->
            state =
              if Map.has_key?(state.connections, socket) do
                put_in(state.connections[socket].recv_buffer, rest)
              else
                state
              end

            Enum.reduce(frames, state, fn frame, acc ->
              if Map.has_key?(acc.connections, socket) do
                handle_frame(acc, socket, frame)
              else
                acc
              end
            end)

          :more ->
            state

          {:error, _reason} ->
            remove_connection(state, socket)
        end
    end
  end

  defp handle_frame(state, socket, {:text, payload}) do
    messages = ChannelEx.Wire.decode_any(payload)
    peer = get_in(state.connections, [socket, :peer])
    {peer, outgoing} = ChannelEx.Peer.receive_messages(peer, messages)

    state
    |> put_in([:connections, socket, :peer], peer)
    |> send_socket_outgoing(socket, outgoing)
  end

  defp handle_frame(state, socket, {:ping, payload}) do
    :ok = :gen_tcp.send(socket, ChannelEx.WebSocket.encode_pong(payload, false))
    state
  end

  defp handle_frame(state, _socket, {:pong, _payload}), do: state
  defp handle_frame(state, socket, {:close, _payload}), do: remove_connection(state, socket)

  defp send_socket_outgoing(state, _socket, []), do: state

  defp send_socket_outgoing(state, socket, outgoing) do
    if Map.has_key?(state.connections, socket) do
      payload =
        outgoing
        |> ChannelEx.Wire.encode!()
        |> IO.iodata_to_binary()
        |> then(&ChannelEx.WebSocket.encode_text(&1, false))

      :ok = :gen_tcp.send(socket, payload)
    end

    state
  end

  defp remove_connection(state, socket) do
    case Map.pop(state.connections, socket) do
      {nil, _connections} ->
        state

      {connection, connections} ->
        if Map.has_key?(connection, :peer) do
          run_server_callback(
            Keyword.get(state.options, :on_disconnect),
            connection.peer.state_ref,
            connection.peer.sender
          )

          ChannelEx.Core.run_disconnect(
            state.channel,
            connection.peer.state_ref,
            connection.peer.sender
          )

          ChannelEx.ConnectionState.stop(connection.peer.state_ref)
        end

        :gen_tcp.close(socket)
        %{state | connections: connections}
    end
  end

  defp build_state(options, request) do
    state_builder = Keyword.get(options, :state)
    merged = Map.merge(request.query, request.headers)

    case state_builder do
      fun when is_function(fun, 1) -> fun.(merged)
      nil -> %{}
      other -> other
    end
  end

  defp run_server_callback(nil, _state_ref, _sender), do: :ok

  defp run_server_callback(fun, state_ref, sender) when is_function(fun, 1) do
    payload = %{state: state_ref, sender: sender}

    try do
      fun.(payload)
    rescue
      _ -> :ok
    catch
      _, _ -> :ok
    end
  end

  defp accept_loop(listener, owner) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        :ok = :gen_tcp.controlling_process(socket, owner)
        send(owner, {:accepted, socket})
        accept_loop(listener, owner)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        send(owner, {:accept_error, reason})
    end
  end

  defp parse_address(address) when is_integer(address), do: {{127, 0, 0, 1}, address}

  defp parse_address(address) when is_binary(address) do
    case String.split(address, ":") do
      [host, port] -> {parse_host(host), String.to_integer(port)}
      _ -> {{127, 0, 0, 1}, 80}
    end
  end

  defp parse_host(host) do
    host
    |> String.split(".")
    |> Enum.map(&String.to_integer/1)
    |> List.to_tuple()
  rescue
    _ -> {127, 0, 0, 1}
  end

  defp listener_port(listener) do
    {:ok, {_, port}} = :inet.sockname(listener)
    port
  end
end
