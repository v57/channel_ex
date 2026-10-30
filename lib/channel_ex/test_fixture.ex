defmodule ChannelEx.Test.TSFixture do
  @moduledoc false

  @doc """
  Starts `../channel/tests/server.ts` on `port` (WebSocket on TCP, QUIC on UDP) and waits for
  it. When a fixture already listens there, that one answers and the new one exits.
  """
  def start!(port \\ 2049) do
    bun = System.find_executable("bun") || raise "bun executable not found"
    channel_dir = Path.expand("../../../channel", __DIR__)

    fixture =
      Port.open(
        {:spawn_executable, bun},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          {:cd, String.to_charlist(channel_dir)},
          {:env, [{~c"CHANNEL_TEST_PORT", ~c"#{port}"}]},
          args: ["tests/server.ts"]
        ]
      )

    wait_until_ready!(~c"127.0.0.1", port)
    fixture
  end

  def stop(port) do
    if Port.info(port) do
      Port.close(port)
    end

    :ok
  end

  def wait_until_ready!(host \\ ~c"127.0.0.1", port \\ 2049, tries_left \\ 120) do
    do_wait_until_ready!(host, port, tries_left)
  end

  defp do_wait_until_ready!(_host, _port, 0),
    do: raise("timed out waiting for tests/server.ts")

  defp do_wait_until_ready!(host, port, tries_left) do
    case :gen_tcp.connect(host, port, [:binary, {:active, false}], 100) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        :ok

      _ ->
        Process.sleep(50)
        do_wait_until_ready!(host, port, tries_left - 1)
    end
  end

  @doc """
  Runs `test/support/ts_client.ts`, the TypeScript reference's client, against an Elixir server
  at `address` and returns `{output, status}`; the output's last line is what came back, as JSON.
  """
  def run_client(address, cert \\ nil) do
    bun = System.find_executable("bun") || raise "bun executable not found"
    script = Path.expand("../../test/support/ts_client.ts", __DIR__)
    System.cmd(bun, [script, address | List.wrap(cert)])
  end
end
