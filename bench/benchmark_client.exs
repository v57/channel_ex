Mix.Task.run("app.start")

defmodule ChannelEx.Benchmark.SignalHandler do
  @behaviour :gen_event

  @impl true
  def init(owner), do: {:ok, owner}

  @impl true
  def handle_event(:sigterm, owner) do
    send(owner, :benchmark_sigterm)
    {:ok, owner}
  end

  def handle_event(_event, owner), do: {:ok, owner}

  @impl true
  def handle_call(_request, owner), do: {:ok, :ok, owner}

  @impl true
  def handle_info(_info, owner), do: {:ok, owner}

  @impl true
  def terminate(_reason, _owner), do: :ok

  @impl true
  def code_change(_old_vsn, owner, _extra), do: {:ok, owner}
end

defmodule ChannelEx.Benchmark.ClientProcess do
  def run do
    install_sigterm_handler()

    host = System.get_env("BENCH_HOST") || "127.0.0.1"
    port = env_int!("BENCH_PORT")
    clients_count = env_int!("BENCH_CLIENTS")
    total_requests = env_int!("BENCH_TOTAL_REQUESTS")
    max_in_flight = env_optional_positive_int("BENCH_MAX_IN_FLIGHT") || 4_096

    clients = connect_all(host, port, clients_count)

    emit(%{
      "type" => "ready",
      "role" => "client",
      "clients" => clients_count,
      "pid" => System.pid()
    })

    started_us = System.monotonic_time(:microsecond)
    {completed, errors} = run_burst(clients, total_requests, max_in_flight)

    elapsed_seconds = (System.monotonic_time(:microsecond) - started_us) / 1_000_000

    Enum.each(clients, &ChannelEx.ClientSender.stop/1)

    emit(%{
      "type" => "result",
      "role" => "client",
      "completed" => completed,
      "errors" => errors,
      "elapsedSeconds" => elapsed_seconds,
      "pid" => System.pid()
    })

    receive do
      :benchmark_sigterm -> :ok
    after
      0 -> :ok
    end
  end

  defp connect_all(host, port, count) do
    Enum.map(1..count, fn _ ->
      address = "ws://#{host}:#{port}"
      client = ChannelEx.connect(ChannelEx.new(), address)
      wait_for_connect!(client.ws, 400)
      client
    end)
  end

  defp run_burst(clients, total_requests, max_in_flight) do
    do_run_burst(clients, total_requests, max_in_flight, 0, 0, 0, 0)
  end

  defp do_run_burst(clients, total_requests, max_in_flight, sent, in_flight, completed, errors) do
    {sent, in_flight} = fill_pipeline(clients, total_requests, max_in_flight, sent, in_flight)

    cond do
      sent >= total_requests and in_flight == 0 ->
        {completed, errors}

      true ->
        receive do
          {_ref, %{body: 5}} ->
            do_run_burst(
              clients,
              total_requests,
              max_in_flight,
              sent,
              in_flight - 1,
              completed + 1,
              errors
            )

          {_ref, %{error: _error}} ->
            do_run_burst(
              clients,
              total_requests,
              max_in_flight,
              sent,
              in_flight - 1,
              completed,
              errors + 1
            )

          {_ref, _other} ->
            do_run_burst(
              clients,
              total_requests,
              max_in_flight,
              sent,
              in_flight - 1,
              completed,
              errors + 1
            )
        after
          30_000 ->
            {completed, errors + in_flight + max(total_requests - sent, 0)}
        end
    end
  end

  defp fill_pipeline(clients, total_requests, max_in_flight, sent, in_flight) do
    cond do
      sent >= total_requests ->
        {sent, in_flight}

      in_flight >= max_in_flight ->
        {sent, in_flight}

      true ->
        client = Enum.at(clients, rem(sent, length(clients)))
        _request = ChannelEx.ClientSender.request(client, "text/count", %{"text" => "hello"})
        fill_pipeline(clients, total_requests, max_in_flight, sent + 1, in_flight + 1)
    end
  end

  defp wait_for_connect!(_ws, 0), do: raise("benchmark client failed to connect")

  defp wait_for_connect!(ws, retries) do
    case ChannelEx.Client.Socket.status(ws) do
      %{connected: true} ->
        :ok

      _ ->
        Process.sleep(10)
        wait_for_connect!(ws, retries - 1)
    end
  end

  defp install_sigterm_handler do
    try do
      :gen_event.delete_handler(:erl_signal_server, :erl_signal_handler, [])
    catch
      _, _ -> :ok
    end

    :ok = :gen_event.add_handler(:erl_signal_server, ChannelEx.Benchmark.SignalHandler, self())
  end

  defp emit(map), do: IO.puts(ChannelEx.JSON.encode!(map))

  defp env_int!(name) do
    case System.get_env(name) do
      nil ->
        raise("missing #{name}")

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _ -> raise("invalid #{name}: #{inspect(value)}")
        end
    end
  end

  defp env_optional_positive_int(name) do
    case System.get_env(name) do
      nil ->
        nil

      "" ->
        nil

      value ->
        case Integer.parse(value) do
          {parsed, ""} when parsed > 0 -> parsed
          _ -> raise("invalid #{name}: #{inspect(value)}")
        end
    end
  end
end

ChannelEx.Benchmark.ClientProcess.run()
