defmodule ChannelEx.Test.TSFixture do
  @moduledoc false

  def start! do
    bun = System.find_executable("bun") || raise "bun executable not found"
    channel_dir = Path.expand("../../../channel", __DIR__)

    port =
      Port.open(
        {:spawn_executable, bun},
        [
          :binary,
          :exit_status,
          :stderr_to_stdout,
          {:cd, String.to_charlist(channel_dir)},
          args: ["tests/server.ts"]
        ]
      )

    wait_until_ready!()
    port
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
    do: raise("timed out waiting for tests/server.ts on 127.0.0.1:2049")

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
end
