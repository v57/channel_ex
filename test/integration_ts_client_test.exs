defmodule ChannelEx.IntegrationTSClientTest do
  # The TypeScript reference's client (test/support/ts_client.ts) calls an Elixir WebSocket
  # server; quic_test.exs runs the same client over QUIC
  use ExUnit.Case, async: false

  @moduletag :interop

  setup do
    test = self()
    ticks = ChannelEx.Subscription.new()

    channel =
      ChannelEx.new()
      |> ChannelEx.post("hello", fn _ -> "world" end)
      |> ChannelEx.post("echo", fn %{body: body} -> body end)
      |> ChannelEx.post("nil", fn _ -> nil end)
      |> ChannelEx.post("name", fn %{state: state} ->
        ChannelEx.ConnectionState.get(state)["x-name"]
      end)
      |> ChannelEx.post("slow", fn _ ->
        Process.sleep(300)
        "slow"
      end)
      |> ChannelEx.post("tick", fn _ ->
        ChannelEx.Subscription.send(ticks, "a", "tick")
        nil
      end)
      |> ChannelEx.stream("count", fn _ -> [0, 1, 2] end)
      |> ChannelEx.events(%{ticks: ticks})

    listener =
      ChannelEx.listen(channel, 0,
        state: fn headers -> headers end,
        on_connect: fn %{sender: sender} -> send(test, {:connected, sender}) end,
        on_disconnect: fn _ -> send(test, :disconnected) end
      )

    on_exit(fn -> ChannelEx.ServerListener.stop(listener) end)
    %{listener: listener}
  end

  test "client calls this server", %{listener: listener} do
    test = self()

    task =
      Task.async(fn -> ChannelEx.Test.TSFixture.run_client("ws://127.0.0.1:#{listener.port}") end)

    # Server to client, over the same connection
    assert_receive {:connected, sender}, 10_000
    spawn(fn -> send(test, {:callback, ChannelEx.Sender.send(sender, "hello")}) end)
    assert_receive {:callback, {:ok, "ts world"}}, 5_000

    {output, status} = Task.await(task, 30_000)
    assert status == 0, output

    # nil reaches the TypeScript client as null, not as the string "nil"
    assert %{
             "hello" => "world",
             "echo" => %{"a" => [1, "b"]},
             "name" => "ts",
             "nil" => nil,
             "values" => [0, 1, 2],
             "event" => "tick",
             "snapshot" => nil
           } ==
             output
             |> String.trim()
             |> String.split("\n")
             |> List.last()
             |> ChannelEx.JSON.decode!()

    assert_receive :disconnected, 5_000
  end
end
