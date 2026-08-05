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

defmodule ChannelEx.Benchmark.ServerProcess do
  def run do
    install_sigterm_handler()
    port = env_int!("BENCH_PORT")
    requests = :atomics.new(1, [])

    listener =
      ChannelEx.new()
      |> ChannelEx.post("text/count", fn %{body: %{"text" => text}} ->
        :atomics.add_get(requests, 1, 1)
        String.length(text)
      end)
      |> ChannelEx.listen(port)

    emit(%{
      "type" => "ready",
      "role" => "server",
      "port" => listener.port,
      "pid" => System.pid()
    })

    await_shutdown(listener, requests)
  end

  defp await_shutdown(listener, requests) do
    receive do
      :benchmark_sigterm ->
        ChannelEx.ServerListener.stop(listener)

        emit(%{
          "type" => "result",
          "role" => "server",
          "requests" => :atomics.get(requests, 1),
          "pid" => System.pid()
        })

        System.stop(0)

      _other ->
        await_shutdown(listener, requests)
    end
  end

  defp emit(map), do: IO.puts(ChannelEx.JSON.encode!(map))

  defp install_sigterm_handler do
    try do
      :gen_event.delete_handler(:erl_signal_server, :erl_signal_handler, [])
    catch
      _, _ -> :ok
    end

    :ok = :gen_event.add_handler(:erl_signal_server, ChannelEx.Benchmark.SignalHandler, self())
  end

  defp env_int!(name) do
    case System.get_env(name) do
      nil ->
        raise("missing #{name}")

      value ->
        case Integer.parse(value) do
          {parsed, ""} -> parsed
          _ -> raise("invalid #{name}: #{inspect(value)}")
        end
    end
  end
end

ChannelEx.Benchmark.ServerProcess.run()
