if Code.ensure_loaded?(:quicer) do
  defmodule ChannelEx.Quic.Server do
    @moduledoc false
    # Owns a QUIC listener (channel/docs/quic.md) and a DynamicSupervisor of
    # ChannelEx.Quic.ServerConnection processes, one per connection. quicer refuses a connection
    # when no acceptor waits, so a few accepts are always queued; each new connection queues
    # another. Like ChannelEx.ServerOwner it only fans local subscription events out.

    use GenServer

    alias ChannelEx.Quic.TLS

    @acceptors 16
    @idle_timeout_ms 30_000

    def port(pid), do: GenServer.call(pid, :port)

    @doc """
    Listens on UDP and returns a `%ChannelEx.ServerListener{}` linked to the caller; raises
    when the key, the certificate or the address can't be used.
    """
    def listen(channel, address, options) do
      Keyword.get(options, :key) || raise ArgumentError, "listen_quic needs a :key (PEM)"
      Keyword.get(options, :cert) || raise ArgumentError, "listen_quic needs a :cert (PEM)"

      case GenServer.start(__MODULE__, {channel, address, options}) do
        {:ok, owner} ->
          Process.link(owner)
          %ChannelEx.ServerListener{owner: owner, channel: channel, port: port(owner)}

        {:error, reason} ->
          raise ArgumentError, "can't listen on QUIC #{inspect(address)}: #{inspect(reason)}"
      end
    end

    @impl true
    def init({channel, address, options}) do
      {:ok, _} = Application.ensure_all_started(:quicer)
      key = Keyword.fetch!(options, :key)
      cert = Keyword.fetch!(options, :cert)

      result =
        TLS.with_pem_files([key, cert], fn [keyfile, certfile] ->
          :quicer.listen(listen_on(address), %{
            alpn: [TLS.alpn()],
            keyfile: keyfile,
            certfile: certfile,
            verify: :none,
            idle_timeout_ms: @idle_timeout_ms,
            # One stream per connection: the client's first; it may not open more or any
            # unidirectional ones
            peer_bidi_stream_count: 1,
            peer_unidi_stream_count: 0
          })
        end)

      case result do
        {:ok, listener} ->
          {:ok, {_ip, port}} = :quicer.sockname(listener)
          for _ <- 1..@acceptors, do: {:ok, _} = :quicer.async_accept(listener, %{active: true})
          {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
          owner = self()

          publisher_refs =
            ChannelEx.Core.register_publishers(channel, fn event ->
              send(owner, {:channel_ex_local_publish, event.topic, event.body})
            end)

          {:ok,
           %{
             channel: channel,
             options: options,
             listener: listener,
             port: port,
             supervisor: supervisor,
             connections: %{},
             publisher_refs: publisher_refs
           }}

        {:error, reason} ->
          {:stop, reason}

        {:error, reason, detail} ->
          {:stop, {reason, detail}}
      end
    end

    @impl true
    def handle_call(:port, _from, state), do: {:reply, state.port, state}

    @impl true
    def handle_info({:quic, :new_conn, conn, _props}, state) do
      _ = :quicer.async_accept(state.listener, %{active: true})
      spec = {ChannelEx.Quic.ServerConnection, {state.channel, state.options}}

      with {:ok, pid} <- DynamicSupervisor.start_child(state.supervisor, spec),
           :ok <- :quicer.controlling_process(conn, pid) do
        ChannelEx.Quic.ServerConnection.attach(pid, conn)
        ref = Process.monitor(pid)
        {:noreply, %{state | connections: Map.put(state.connections, pid, ref)}}
      else
        _ ->
          :quicer.async_shutdown_connection(conn, 0, 0)
          {:noreply, state}
      end
    end

    def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
      {:noreply, %{state | connections: Map.delete(state.connections, pid)}}
    end

    def handle_info({:channel_ex_local_publish, topic, body}, state) do
      Enum.each(Map.keys(state.connections), fn pid ->
        send(pid, {:channel_ex_local_publish, topic, body})
      end)

      {:noreply, state}
    end

    def handle_info(:channel_ex_stop, state), do: {:stop, :normal, state}
    def handle_info(_message, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state) do
      ChannelEx.Core.cleanup_publishers(state.publisher_refs)
      # Stops every connection; each one runs its on_disconnect callbacks in terminate/2.
      if Process.alive?(state.supervisor) do
        Process.unlink(state.supervisor)
        DynamicSupervisor.stop(state.supervisor, :shutdown)
      end

      :quicer.close_listener(state.listener)
      :ok
    end

    defp listen_on(port) when is_integer(port), do: ~c"127.0.0.1:#{port}"
    defp listen_on(address) when is_binary(address), do: String.to_charlist(address)
  end

  defmodule ChannelEx.Quic.ServerConnection do
    @moduledoc false
    # One process per QUIC connection, the owner of its connection and stream. The first frame
    # must be the hello; its headers (names lowercased) build the connection state, as the
    # WebSocket upgrade headers do. A frame over the limit, invalid JSON or a missing hello
    # closes the connection.

    use GenServer, restart: :temporary

    alias ChannelEx.Quic.{Frame, Owner}

    def start_link(args), do: GenServer.start_link(__MODULE__, args)

    def attach(pid, conn), do: send(pid, {:channel_ex_attach, conn})

    @impl true
    def init({channel, options}) do
      # Trap exits so the supervisor's shutdown runs terminate/2 (on_disconnect callbacks).
      Process.flag(:trap_exit, true)

      {:ok,
       %{
         channel: channel,
         options: options,
         max_payload: Keyword.get(options, :max_payload, Frame.default_max_payload()),
         conn: nil,
         stream: nil,
         recv_buffer: <<>>,
         peer: nil,
         open: true
       }}
    end

    @impl true
    def handle_info({:channel_ex_attach, conn}, state) do
      with {:ok, _} <- :quicer.async_accept_stream(conn, %{active: true}),
           :ok <- :quicer.async_handshake(conn) do
        {:noreply, %{state | conn: conn}}
      else
        _ -> {:stop, :normal, %{state | conn: conn}}
      end
    end

    def handle_info({:quic, :new_stream, stream, props}, %{stream: nil} = state) do
      # A stream that arrived before the acceptor was queued starts passive
      if Map.get(props, :is_orphan, false), do: :quicer.setopt(stream, :active, true)
      {:noreply, %{state | stream: stream}}
    end

    def handle_info({:quic, :new_stream, extra, _props}, state) do
      :quicer.async_shutdown_stream(extra)
      {:noreply, state}
    end

    def handle_info({:quic, data, stream, _props}, %{stream: stream} = state)
        when is_binary(data) do
      %{state | recv_buffer: state.recv_buffer <> data}
      |> process_recv_buffer()
      |> continue()
    end

    def handle_info({:quic, event, stream, _props}, %{stream: stream} = state)
        when event in [:peer_send_shutdown, :peer_send_aborted, :stream_closed] do
      {:stop, :normal, state}
    end

    def handle_info({:quic, event, conn, _props}, %{conn: conn} = state)
        when event in [:transport_shutdown, :shutdown, :closed] do
      {:stop, :normal, state}
    end

    # Sender.stop/1 on a server-side sender closes this connection only.
    def handle_info(:channel_ex_stop, state), do: {:stop, :normal, state}

    def handle_info(message, %{peer: peer} = state) when peer != nil do
      case Owner.apply_message(peer, message) do
        {:ok, peer, outgoing} ->
          {peer, outgoing} = Owner.drain(peer, Enum.reverse(outgoing))
          %{state | peer: peer} |> send_outgoing(Enum.reverse(outgoing)) |> continue()

        :unknown ->
          {:noreply, state}
      end
    end

    def handle_info(_message, state), do: {:noreply, state}

    @impl true
    def terminate(_reason, state) do
      if peer = state.peer do
        Owner.fail_pending(peer.pending)
        run_callback(Keyword.get(state.options, :on_disconnect), peer.state_ref, peer.sender)
        ChannelEx.Core.run_disconnect(state.channel, peer.state_ref, peer.sender)
        ChannelEx.ConnectionState.stop(peer.state_ref)
      end

      if state.conn, do: :quicer.async_shutdown_connection(state.conn, 0, 0)
      :ok
    end

    defp continue(%{open: true} = state), do: {:noreply, state}
    defp continue(state), do: {:stop, :normal, state}

    defp process_recv_buffer(state) do
      case Frame.decode(state.recv_buffer, state.max_payload) do
        {:ok, payloads, rest} ->
          Enum.reduce(payloads, %{state | recv_buffer: rest}, fn payload, acc ->
            if acc.open, do: handle_payload(acc, payload), else: acc
          end)

        {:error, _reason} ->
          %{state | open: false}
      end
    end

    defp handle_payload(%{peer: nil} = state, payload) do
      with {:ok, headers} <- parse_hello(payload),
           {:ok, value} <- build_state(state.options, headers) do
        {:ok, state_ref} = ChannelEx.ConnectionState.start_link(value)
        peer = ChannelEx.Peer.new(state.channel, self(), {:server_quic, state.conn}, state_ref)
        run_callback(Keyword.get(state.options, :on_connect), state_ref, peer.sender)
        %{state | peer: peer}
      else
        _ -> %{state | open: false}
      end
    end

    defp handle_payload(state, payload) do
      case ChannelEx.Wire.decode_messages(payload) do
        {:ok, messages} ->
          {peer, outgoing} = ChannelEx.Peer.receive_messages(state.peer, messages)
          send_outgoing(%{state | peer: peer}, outgoing)

        :error ->
          %{state | open: false}
      end
    end

    defp parse_hello(payload) do
      case ChannelEx.JSON.decode!(payload) do
        %{"hello" => hello} when is_map(hello) ->
          {:ok,
           for {key, value} <- hello, is_binary(value), into: %{} do
             {String.downcase(key), value}
           end}

        _ ->
          :error
      end
    rescue
      _ -> :error
    catch
      _, _ -> :error
    end

    defp build_state(options, headers) do
      case Keyword.get(options, :state) do
        fun when is_function(fun, 1) -> {:ok, fun.(headers)}
        nil -> {:ok, %{}}
        other -> {:ok, other}
      end
    rescue
      _ -> :error
    catch
      _, _ -> :error
    end

    defp send_outgoing(state, []), do: state

    defp send_outgoing(%{open: true} = state, outgoing) do
      payload = Frame.encode(ChannelEx.Wire.encode!(outgoing))

      case :quicer.async_send(state.stream, payload) do
        {:ok, _} -> state
        _ -> %{state | open: false}
      end
    end

    defp send_outgoing(state, _outgoing), do: state

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
  end
end
