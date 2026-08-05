defmodule ChannelEx.IntegrationTSServerTest do
  use ExUnit.Case, async: false

  setup_all do
    fixture = ChannelEx.Test.TSFixture.start!()
    {:ok, values_sent} = Agent.start_link(fn -> 0 end)

    names = ChannelEx.Subscription.new() |> ChannelEx.Subscription.set_body(fn value -> value end)

    client_channel =
      ChannelEx.new()
      |> ChannelEx.post("hello", fn _ -> "client world" end)
      |> ChannelEx.stream("stream/values", fn _ ->
        [0, 1, 2]
      end)
      |> ChannelEx.stream("stream/cancel", fn _ ->
        Enum.map(0..9, fn value ->
          Agent.update(values_sent, &(&1 + 1))
          value
        end)
      end)
      |> ChannelEx.events(%{names: names})

    client = ChannelEx.connect(client_channel, 2049)
    wait_for_connect!(client.ws, 200)

    on_exit(fn ->
      ChannelEx.ClientSender.stop(client)
      ChannelEx.Test.TSFixture.stop(fixture)
    end)

    %{client: client, names: names, values_sent: values_sent}
  end

  defp wait_for_connect!(_ws, 0), do: raise("client websocket did not connect in time")

  defp wait_for_connect!(ws, retries) do
    case ChannelEx.Client.Socket.status(ws) do
      %{connected: true} ->
        :ok

      _ ->
        Process.sleep(25)
        wait_for_connect!(ws, retries - 1)
    end
  end

  test "post/hello", %{client: client} do
    assert {:ok, "world"} = ChannelEx.send(client, "hello")
  end

  test "post/echo", %{client: client} do
    value = :rand.uniform()
    assert {:ok, ^value} = ChannelEx.send(client, "echo", value)
  end

  test "post/empty", %{client: client} do
    assert {:ok, nil} = ChannelEx.ClientSender.send(client, "empty")
  end

  test "post/notfound", %{client: client} do
    assert {:error, "api not found"} = ChannelEx.ClientSender.send(client, "notfound")
  end

  test "subscribe/notfound", %{client: client} do
    assert {:error, "subscription not found"} =
             ChannelEx.ClientSender.subscribe(client, "notfound", "test", fn _ -> :ok end)
  end

  test "stream/values", %{client: client} do
    assert [0, 1, 2] = client |> ChannelEx.values("stream/values") |> Enum.to_list()
  end

  test "stream/cancel", %{client: client} do
    assert [0, 1, 2] = client |> ChannelEx.values("stream/cancel") |> Enum.take(3)
  end

  test "server/post mirror", %{client: client} do
    assert {:ok, "client world"} = ChannelEx.ClientSender.send(client, "mirror")
  end

  test "server/stream mirror", %{client: client} do
    assert [0, 1, 2] = client |> ChannelEx.values("mirror/stream") |> Enum.to_list()
  end

  test "server/stream/cancel mirror", %{client: client, values_sent: values_sent} do
    Agent.update(values_sent, fn _ -> 0 end)

    assert [0, 1, 2] =
             client |> ChannelEx.values("mirror/stream/cancel") |> Enum.take(3)

    Process.sleep(50)
    assert Agent.get(values_sent, & &1) == 10
  end

  test "server/subscribe mirror events", %{client: client, names: names} do
    assert {:ok, nil} = ChannelEx.ClientSender.send(client, "mirror/events")

    Process.sleep(20)
    ChannelEx.Subscription.send(names, "2", "po")
    ChannelEx.Subscription.send(names, "1", "h")
    Process.sleep(20)
    ChannelEx.Subscription.send(names, "1", "we")
    Process.sleep(20)
    assert {:ok, "world"} = ChannelEx.ClientSender.send(client, "hello")
  end

  test "state/auth and auth/name", %{client: _client} do
    isolated = ChannelEx.connect(ChannelEx.new(), 2049)
    wait_for_connect!(isolated.ws, 200)
    assert {:ok, "tester"} = ChannelEx.ClientSender.send(isolated, "auth", %{"name" => "tester"})
    assert {:ok, "tester"} = ChannelEx.ClientSender.send(isolated, "auth/name")
    ChannelEx.ClientSender.stop(isolated)
  end

  test "state/unauthorized", %{client: _client} do
    isolated = ChannelEx.connect(ChannelEx.new(), 2049)
    wait_for_connect!(isolated.ws, 200)
    assert {:error, "unauthorized"} = ChannelEx.ClientSender.send(isolated, "auth/name")
    ChannelEx.ClientSender.stop(isolated)
  end
end
