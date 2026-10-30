if Code.ensure_loaded?(:quicer) do
  defmodule ChannelEx.Quic.Client do
    @moduledoc false
    # Client side of `quic://host:port` (channel/docs/quic.md): one QUIC connection, from its own UDP
    # socket (msquic doesn't share a client binding unless asked), with one bidirectional stream that
    # carries length-prefixed JSON messages. Behaves like ChannelEx.Client.Socket otherwise: the
    # process owns the connection's Peer, and it stops when the connection closes.

    use GenServer

    alias ChannelEx.Quic.{Frame, TLS}

    @idle_timeout_ms 30_000
    @keepalive_ms 15_000

    @doc """
    Connects and returns a `%ChannelEx.ClientSender{}`, or `{:error, reason}` when the address,
    the certificate or the handshake fails. The caller is linked only after the connection is up.
    """
    def connect(channel, address, options) do
      ref = make_ref()

      opts = [
        channel: channel,
        url: address,
        headers: Keyword.get(options, :headers),
        on_connect: Keyword.get(options, :on_connect),
        state_ref: Keyword.get(options, :state, %{}),
        connect_timeout: Keyword.get(options, :connect_timeout, 5_000),
        max_payload: Keyword.get(options, :max_payload, Frame.default_max_payload()),
        quic: Keyword.get(options, :quic, []),
        caller: self(),
        ref: ref
      ]

      case GenServer.start_link(__MODULE__, opts) do
        {:ok, pid} ->
          try do
            %ChannelEx.ClientSender{sender: GenServer.call(pid, :sender), ws: pid}
          catch
            # The server closed the connection right after the handshake.
            :exit, _reason -> {:error, :closed}
          end

        :ignore ->
          receive do
            {^ref, {:error, _reason} = error} -> error
          after
            0 -> {:error, :closed}
          end
      end
    end

    @impl true
    def init(opts) do
      case open(opts) do
        {:ok, conn, stream} ->
          {:ok, start_peer(opts, conn, stream)}

        {:error, _reason} = error ->
          # :ignore keeps the linked caller alive; the reason goes to it as a message.
          send(Keyword.fetch!(opts, :caller), {Keyword.fetch!(opts, :ref), error})
          :ignore
      end
    end

    defp open(opts) do
      with {:ok, _} <- Application.ensure_all_started(:quicer),
           {:ok, host, port} <- parse_url(Keyword.fetch!(opts, :url)),
           {:ok, tls} <- tls_options(Keyword.get(opts, :quic, [])),
           {:ok, conn} <- dial(host, port, tls, Keyword.fetch!(opts, :connect_timeout)) do
        with {:ok, stream} <- :quicer.start_stream(conn, %{active: true}),
             {:ok, _} <- send_frame(stream, hello(Keyword.get(opts, :headers))) do
          {:ok, conn, stream}
        else
          error ->
            :quicer.async_shutdown_connection(conn, 0, 0)
            normalize_error(error)
        end
      else
        error -> normalize_error(error)
      end
    end

    defp normalize_error({:error, reason}), do: {:error, reason}
    defp normalize_error({:error, reason, detail}), do: {:error, {reason, detail}}
    defp normalize_error(other), do: {:error, other}

    defp parse_url(url) do
      case URI.parse(url) do
        %URI{scheme: "quic", host: host, port: port}
        when is_binary(host) and host != "" and is_integer(port) ->
          {:ok, host, port}

        _ ->
          {:error, {:invalid_address, url}}
      end
    end

    defp tls_options(quic) do
      quic = Map.new(quic)

      case TLS.parse_fingerprint(Map.get(quic, :fingerprint)) do
        :error -> {:error, {:invalid_fingerprint, Map.get(quic, :fingerprint)}}
        pin -> {:ok, %{ca: Map.get(quic, :ca), pin: pin, servername: Map.get(quic, :servername)}}
      end
    end

    defp dial(host, port, tls, timeout) do
      ip = parse_ip(host)

      conn_opts = %{
        alpn: [TLS.alpn()],
        # msquic checks the host (a DNS name or an IP address) against the certificate; a pin
        # replaces those checks, as in the TypeScript client
        verify: if(tls.pin, do: :none, else: :peer),
        idle_timeout_ms: @idle_timeout_ms,
        handshake_idle_timeout_ms: timeout,
        peer_bidi_stream_count: 0,
        peer_unidi_stream_count: 0
      }

      # A pin is checked here, in the handshake (quicer's custom verification)
      conn_opts = if tls.pin, do: Map.put(conn_opts, :custom_verify, true), else: conn_opts

      TLS.with_pem_files([if(tls.pin, do: nil, else: tls.ca)], fn [cacertfile] ->
        conn_opts =
          if cacertfile, do: Map.put(conn_opts, :cacertfile, cacertfile), else: conn_opts

        case tls.servername do
          nil ->
            :quicer.connect(String.to_charlist(host), port, conn_opts, timeout)

          servername ->
            # msquic sends and checks the name it dials, so the address is set beforehand
            with {:ok, address} <- resolve(host, ip),
                 {:ok, conn} <- :quicer.open_connection(),
                 :ok <- :quicer.setopt(conn, :remote_address, remote_address(address, port)) do
              :quicer.connect(
                String.to_charlist(servername),
                port,
                Map.put(conn_opts, :handle, conn),
                timeout
              )
            end
        end
      end)
      |> case do
        {:ok, conn, {der, _chain}} when tls.pin != nil ->
          complete_pinned(conn, der, tls.pin, timeout)

        {:ok, conn} ->
          {:ok, conn}

        other ->
          other
      end
    end

    defp complete_pinned(conn, der, pin, timeout) do
      accepted = :crypto.hash(:sha256, der) == pin
      # TLS alert 42: bad_certificate
      _ = :quicer.complete_cert_validation(conn, accepted, 42)

      receive do
        {:quic, :connected, ^conn, _props} when accepted ->
          {:ok, conn}

        {:quic, :transport_shutdown, ^conn, _reason} when not accepted ->
          {:error, :certificate_fingerprint_mismatch}

        {:quic, :transport_shutdown, ^conn, reason} ->
          {:error, {:transport_down, reason}}
      after
        timeout ->
          :quicer.async_shutdown_connection(conn, 0, 0)
          {:error, if(accepted, do: :timeout, else: :certificate_fingerprint_mismatch)}
      end
    end

    defp parse_ip(host) do
      case :inet.parse_strict_address(String.to_charlist(host)) do
        {:ok, ip} -> ip
        _ -> nil
      end
    end

    defp resolve(_host, ip) when ip != nil, do: {:ok, ip}

    defp resolve(host, nil) do
      case :inet.getaddr(String.to_charlist(host), :inet) do
        {:ok, ip} -> {:ok, ip}
        _ -> :inet.getaddr(String.to_charlist(host), :inet6)
      end
    end

    defp remote_address({_, _, _, _} = ip, port), do: ~c"#{:inet.ntoa(ip)}:#{port}"
    defp remote_address(ip, port), do: ~c"[#{:inet.ntoa(ip)}]:#{port}"

    defp hello(headers_fun) do
      ChannelEx.JSON.encode!(%{"hello" => headers(headers_fun)})
    end

    defp headers(nil), do: %{}

    defp headers(fun) when is_function(fun, 0) do
      case fun.() do
        headers when is_map(headers) ->
          for {key, value} <- headers, is_binary(value), into: %{}, do: {to_string(key), value}

        _ ->
          %{}
      end
    rescue
      _ -> %{}
    end

    defp send_frame(stream, payload), do: :quicer.async_send(stream, Frame.encode(payload))

    defp start_peer(opts, conn, stream) do
      channel = Keyword.fetch!(opts, :channel)
      {:ok, state_ref} = ChannelEx.ConnectionState.start_link(Keyword.fetch!(opts, :state_ref))
      owner = self()
      peer = ChannelEx.Peer.new(channel, owner, :client, state_ref)

      publisher_refs =
        ChannelEx.Core.register_publishers(channel, fn event ->
          send(owner, {:channel_ex_local_publish, event.topic, event.body})
        end)

      run_on_connect(Keyword.get(opts, :on_connect), peer.sender)
      Process.send_after(self(), :channel_ex_keepalive, @keepalive_ms)

      %{
        conn: conn,
        stream: stream,
        connected: true,
        recv_buffer: <<>>,
        max_payload: Keyword.fetch!(opts, :max_payload),
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

    def handle_call(:sockname, _from, state), do: {:reply, :quicer.sockname(state.conn), state}

    @impl true
    def handle_info({:quic, data, stream, _props}, %{stream: stream} = state)
        when is_binary(data) do
      state
      |> Map.update!(:recv_buffer, &(&1 <> data))
      |> process_recv_buffer()
      |> continue()
    end

    def handle_info({:quic, event, stream, _props}, %{stream: stream} = state)
        when event in [:peer_send_shutdown, :peer_send_aborted, :stream_closed] do
      {:stop, :normal, %{state | connected: false}}
    end

    def handle_info({:quic, event, conn, _props}, %{conn: conn} = state)
        when event in [:transport_shutdown, :shutdown, :closed] do
      {:stop, :normal, %{state | connected: false}}
    end

    def handle_info(:channel_ex_keepalive, state) do
      Process.send_after(self(), :channel_ex_keepalive, @keepalive_ms)
      state |> write(Frame.keepalive()) |> continue()
    end

    def handle_info(:channel_ex_stop, state) do
      {:stop, :normal, %{state | connected: false}}
    end

    def handle_info(message, state) do
      case ChannelEx.Quic.Owner.apply_message(state.peer, message) do
        {:ok, peer, outgoing} -> state |> drain_peer_outgoing(peer, outgoing) |> continue()
        :unknown -> {:noreply, state}
      end
    end

    @impl true
    def terminate(_reason, state) do
      ChannelEx.Quic.Owner.fail_pending(state.peer.pending)
      ChannelEx.Core.cleanup_publishers(state.publisher_refs)
      ChannelEx.Core.run_disconnect(state.peer.channel, state.peer.state_ref, state.peer.sender)
      ChannelEx.ConnectionState.stop(state.peer.state_ref)
      :quicer.async_shutdown_connection(state.conn, 0, 0)
      :ok
    end

    defp continue(%{connected: true} = state), do: {:noreply, state}
    defp continue(state), do: {:stop, :normal, state}

    defp drain_peer_outgoing(state, peer, outgoing) do
      {peer, outgoing} = ChannelEx.Quic.Owner.drain(peer, Enum.reverse(outgoing))
      send_peer_outgoing(%{state | peer: peer}, Enum.reverse(outgoing))
    end

    defp send_peer_outgoing(state, []), do: state

    defp send_peer_outgoing(state, outgoing) do
      write(state, Frame.encode(ChannelEx.Wire.encode!(outgoing)))
    end

    defp write(%{connected: true} = state, iodata) do
      case :quicer.async_send(state.stream, iodata) do
        {:ok, _} -> state
        _ -> %{state | connected: false}
      end
    end

    defp write(state, _iodata), do: state

    defp process_recv_buffer(state) do
      case Frame.decode(state.recv_buffer, state.max_payload) do
        {:ok, payloads, rest} ->
          Enum.reduce(payloads, %{state | recv_buffer: rest}, fn payload, acc ->
            if acc.connected, do: handle_payload(acc, payload), else: acc
          end)

        {:error, _reason} ->
          %{state | connected: false}
      end
    end

    defp handle_payload(state, payload) do
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
  end
end
