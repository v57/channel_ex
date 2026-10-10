defmodule ChannelEx.ServerOwner do
  @moduledoc false
  # Owns the listening socket, the acceptor and a DynamicSupervisor of
  # ChannelEx.ServerConnection processes (one per client). It only fans local
  # subscription events out to the live connections; all per-connection work
  # (handshake, frames, handlers, writes) happens in the connection process.

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
    {:ok, supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    acceptor =
      spawn_link(fn ->
        accept_loop(listener, owner, supervisor, channel, options)
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
       supervisor: supervisor,
       options: options,
       connections: %{},
       publisher_refs: publisher_refs,
       stopping: false
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  @impl true
  def handle_info({:connection, pid}, state) do
    ref = Process.monitor(pid)
    {:noreply, %{state | connections: Map.put(state.connections, pid, ref)}}
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

  def handle_info(:channel_ex_stop, state) do
    {:stop, :normal, %{state | stopping: true}}
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    if state.listener, do: :gen_tcp.close(state.listener)

    if is_pid(state.acceptor) do
      Process.unlink(state.acceptor)
      Process.exit(state.acceptor, :kill)
    end

    # Stops every connection; each one runs its on_disconnect callbacks in terminate/2.
    if is_pid(state.supervisor) and Process.alive?(state.supervisor) do
      Process.unlink(state.supervisor)
      DynamicSupervisor.stop(state.supervisor, :shutdown)
    end

    ChannelEx.Core.cleanup_publishers(state.publisher_refs)
    :ok
  end

  defp accept_loop(listener, owner, supervisor, channel, options) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        start_connection(socket, owner, supervisor, channel, options)
        accept_loop(listener, owner, supervisor, channel, options)

      {:error, :closed} ->
        :ok

      {:error, _reason} ->
        # e.g. :emfile / :enfile: back off instead of giving up on accepting for good.
        Process.sleep(10)
        accept_loop(listener, owner, supervisor, channel, options)
    end
  end

  defp start_connection(socket, owner, supervisor, channel, options) do
    spec = {ChannelEx.ServerConnection, {channel, options, owner}}

    case DynamicSupervisor.start_child(supervisor, spec) do
      {:ok, pid} ->
        case :gen_tcp.controlling_process(socket, pid) do
          :ok ->
            send(owner, {:connection, pid})
            ChannelEx.ServerConnection.attach(pid, socket)

          {:error, _reason} ->
            Process.exit(pid, :kill)
            :gen_tcp.close(socket)
        end

      _ ->
        :gen_tcp.close(socket)
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
