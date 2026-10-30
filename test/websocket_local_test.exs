defmodule ChannelEx.WebSocketLocalTest do
  use ExUnit.Case, async: false

  setup do
    updates =
      ChannelEx.Subscription.new()
      |> ChannelEx.Subscription.set_body(fn value -> value end)

    owner = self()

    channel =
      ChannelEx.new()
      |> ChannelEx.post("hello", fn _ -> "world" end)
      |> ChannelEx.post("echo", fn %{body: body} -> body end)
      |> ChannelEx.post("nil", fn _ -> nil end)
      |> ChannelEx.post("state/check", fn %{state: state_ref} ->
        ChannelEx.ConnectionState.get(state_ref).header_wins
      end)
      |> ChannelEx.post("auth", fn %{body: %{"name" => name}, state: state_ref} ->
        ChannelEx.ConnectionState.update(state_ref, fn state -> Map.put(state, :name, name) end)
        name
      end)
      |> ChannelEx.post("auth/name", fn %{state: state_ref} ->
        case ChannelEx.ConnectionState.get(state_ref) do
          %{name: name} when is_binary(name) -> name
          _ -> raise "unauthorized"
        end
      end)
      |> ChannelEx.post_other(fn path -> String.starts_with?(path, "other/post/") end, fn %{
                                                                                            path:
                                                                                              path
                                                                                          } ->
        path
      end)
      |> ChannelEx.stream("numbers", fn _ -> [0, 1, 2] end)
      |> ChannelEx.stream_other(fn path -> String.starts_with?(path, "other/stream/") end, fn _ ->
        [7, 8]
      end)
      |> ChannelEx.events(%{updates: updates})
      |> ChannelEx.on_disconnect(fn _state, _sender -> send(owner, :disconnected) end)

    listener =
      ChannelEx.listen(channel, 0,
        state: fn headers ->
          %{header_wins: Map.get(headers, "name") == "header"}
        end
      )

    client =
      ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{listener.port}/?name=query",
        headers: fn -> %{"name" => "header"} end
      )

    wait_for_connect!(client.ws, 200)

    on_exit(fn ->
      ChannelEx.ClientSender.stop(client)
      ChannelEx.ServerListener.stop(listener)
    end)

    %{client: client, listener: listener, updates: updates}
  end

  test "request response and state", %{client: client} do
    assert {:ok, "world"} = ChannelEx.send(client, "hello")
    assert {:ok, 5} = ChannelEx.send(client, "echo", 5)
    assert {:ok, nil} = ChannelEx.send(client, "nil")
    # A float keeps the body off the fast path, so the JSON library reads it on both sides
    assert {:ok, %{"a" => nil, "b" => 1.5}} =
             ChannelEx.send(client, "echo", %{"a" => nil, "b" => 1.5})

    assert {:ok, true} = ChannelEx.send(client, "state/check")
    assert {:ok, "tester"} = ChannelEx.send(client, "auth", %{"name" => "tester"})
    assert {:ok, "tester"} = ChannelEx.send(client, "auth/name")
    assert {:ok, "other/post/demo"} = ChannelEx.send(client, "other/post/demo")
  end

  test "streams and subscriptions", %{client: client, updates: updates} do
    assert [0, 1, 2] = client |> ChannelEx.values("numbers") |> Enum.to_list()
    assert [7, 8] = client |> ChannelEx.values("other/stream/demo") |> Enum.to_list()

    owner = self()

    {:ok, cancellable} =
      ChannelEx.ClientSender.subscribe(client, "updates", "room", fn body ->
        send(owner, {:event, body})
      end)

    ChannelEx.Subscription.send(updates, "room", "hello")
    assert_receive {:event, "hello"}, 1_000

    cancellable.cancel.()
    Process.sleep(20)
    ChannelEx.Subscription.send(updates, "room", "ignored")
    refute_receive {:event, "ignored"}, 100
  end

  test "serial sends reuse the same owner connection", %{client: client} do
    Enum.each(0..63, fn value ->
      assert {:ok, ^value} = ChannelEx.send(client, "echo", value)
    end)

    assert %{connected: true, pending: 0, gun_pid: nil, ws_ref: nil} =
             ChannelEx.Client.Socket.status(client.ws)
  end

  test "client stop triggers server disconnect cleanup", %{client: client} do
    assert :ok == ChannelEx.ClientSender.stop(client)
    assert_receive :disconnected, 1_000
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
end
