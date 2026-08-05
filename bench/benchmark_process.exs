Mix.Task.run("app.start")

defmodule ChannelEx.Benchmark.ProcessWorker do
  def run do
    total_requests = env_optional_non_negative_int("BENCH_TOTAL_REQUESTS")
    duration_ms =
      if is_nil(total_requests) do
        env_int!("BENCH_DURATION_MS")
      else
        0
      end

    pid = :os.getpid() |> List.to_string() |> String.to_integer()

    sender =
      ChannelEx.new()
      |> ChannelEx.post("sync", fn _ -> 1 end)
      |> ChannelEx.post("async", fn _ -> 1 end)
      |> ChannelEx.process()

    emit(%{
      "type" => "ready",
      "pid" => pid
    })

    if total_requests != nil do
      {completed, errors, elapsed_seconds} = run_fixed_scenario(sender, "sync", total_requests)
      ChannelEx.Sender.stop(sender)

      emit(%{
        "type" => "result",
        "pid" => pid,
        "completed" => completed,
        "errors" => errors,
        "elapsedSeconds" => elapsed_seconds
      })

      :ok
    else
    {sync_completed, sync_errors, sync_elapsed} = run_scenario(sender, "sync", duration_ms)
    {async_completed, async_errors, async_elapsed} = run_scenario(sender, "async", duration_ms)

    ChannelEx.Sender.stop(sender)

    emit(%{
      "type" => "result",
      "pid" => pid,
      "syncCompleted" => sync_completed,
      "syncErrors" => sync_errors,
      "syncElapsedSeconds" => sync_elapsed,
      "asyncCompleted" => async_completed,
      "asyncErrors" => async_errors,
      "asyncElapsedSeconds" => async_elapsed,
      "asyncFallback" => true
    })
    end
  end

  defp run_fixed_scenario(sender, path, total_requests) do
    started = System.monotonic_time(:microsecond)
    do_run_fixed_scenario(sender, path, started, total_requests, 0, 0)
  end

  defp do_run_fixed_scenario(sender, path, started, total_requests, completed, errors) do
    if (completed + errors) >= total_requests do
      elapsed_seconds = (System.monotonic_time(:microsecond) - started) / 1_000_000
      {completed, errors, elapsed_seconds}
    else
      case ChannelEx.Sender.send(sender, path) do
        {:ok, _value} -> do_run_fixed_scenario(sender, path, started, total_requests, completed + 1, errors)
        {:error, _reason} -> do_run_fixed_scenario(sender, path, started, total_requests, completed, errors + 1)
      end
    end
  end

  defp run_scenario(sender, path, duration_ms) do
    started = System.monotonic_time(:microsecond)
    deadline = started + duration_ms * 1_000
    do_run_scenario(sender, path, started, deadline, 0, 0)
  end

  defp do_run_scenario(sender, path, started, deadline, completed, errors) do
    if System.monotonic_time(:microsecond) >= deadline do
      elapsed_seconds = (System.monotonic_time(:microsecond) - started) / 1_000_000
      {completed, errors, elapsed_seconds}
    else
      case ChannelEx.Sender.send(sender, path) do
        {:ok, _value} -> do_run_scenario(sender, path, started, deadline, completed + 1, errors)
        {:error, _reason} -> do_run_scenario(sender, path, started, deadline, completed, errors + 1)
      end
    end
  end

  defp env_int!(key) do
    case Integer.parse(System.get_env(key, "")) do
      {value, ""} when value > 0 ->
        value

      _ ->
        emit(%{
          "type" => "error",
          "message" => "#{key} must be a valid positive integer"
        })

        System.halt(1)
    end
  end

  defp env_optional_non_negative_int(key) do
    raw = System.get_env(key)

    cond do
      is_nil(raw) or raw == "" ->
        nil

      true ->
        case Integer.parse(raw) do
          {value, ""} when value >= 0 ->
            value

          _ ->
            emit(%{
              "type" => "error",
              "message" => "#{key} must be a valid non-negative integer"
            })

            System.halt(1)
        end
    end
  end

  defp emit(map), do: IO.puts(ChannelEx.JSON.encode!(map))
end

ChannelEx.Benchmark.ProcessWorker.run()
