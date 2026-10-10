defmodule ChannelEx.CoreBehaviorTest do
  use ExUnit.Case, async: false

  test "process sender handles direct and dynamic routes" do
    channel =
      ChannelEx.new()
      |> ChannelEx.post("hello", fn _ -> "world" end)
      |> ChannelEx.post("echo", fn %{body: body} -> body end)
      |> ChannelEx.post_other(fn path -> String.starts_with?(path, "dynamic/") end, fn %{
                                                                                         path:
                                                                                           path,
                                                                                         body:
                                                                                           body
                                                                                       } ->
        "#{path}:#{body}"
      end)
      |> ChannelEx.stream("numbers", fn _ -> [1, 2, 3] end)
      |> ChannelEx.stream_other(fn path -> String.ends_with?(path, "/stream") end, fn %{
                                                                                        body: body
                                                                                      } ->
        [body, body + 1]
      end)

    sender = ChannelEx.process(channel)

    assert {:ok, "world"} = ChannelEx.send(sender, "hello")
    assert {:ok, 7} = ChannelEx.send(sender, "echo", 7)
    assert {:ok, "dynamic/demo:ok"} = ChannelEx.send(sender, "dynamic/demo", "ok")
    assert [1, 2, 3] = sender |> ChannelEx.values("numbers") |> Enum.to_list()
    assert [4, 5] = sender |> ChannelEx.values("dynamic/stream", 4) |> Enum.to_list()
  end

  test "merge keeps post_other/stream_other routes under the prefix" do
    match = fn path -> String.starts_with?(path, "dyn/") end

    inner =
      ChannelEx.new()
      |> ChannelEx.post_other(match, fn %{body: body} -> "post:#{body}" end)
      |> ChannelEx.stream_other(fn path -> path == "feed" end, fn _ -> [1] end)

    outer = ChannelEx.merge(ChannelEx.new(), inner, "m")

    assert [{scoped, _}] = outer.other_post_api
    assert scoped.("m/dyn/x")
    refute scoped.("dyn/x")
    assert [{stream_scoped, _}] = outer.other_stream_api
    assert stream_scoped.("m/feed")

    sender = ChannelEx.process(outer)
    assert {:ok, "post:a"} = ChannelEx.send(sender, "m/dyn/x", "a")
    assert {:error, _} = ChannelEx.send(sender, "dyn/x", "a")
    assert [1] = sender |> ChannelEx.values("m/feed") |> Enum.to_list()

    # Without a prefix the matchers are reused as-is
    flat = ChannelEx.merge(ChannelEx.new(), inner)
    assert [{^match, _}] = flat.other_post_api
  end

  test "post_async is a compatibility alias and handlers see no task payload" do
    parent = self()

    channel =
      ChannelEx.new()
      |> ChannelEx.post_async("task-check", fn request ->
        send(parent, {:request, request})
        "ok"
      end)

    sender = ChannelEx.process(channel)

    assert {:ok, "ok"} = ChannelEx.send(sender, "task-check")
    assert_receive {:request, request}, 500
    assert request.task == nil
    cancelled? = request.cancelled?
    assert false == cancelled?.()
  end

  test "subscription body formatter no longer overwrites topic formatter" do
    subscription =
      ChannelEx.Subscription.new()
      |> ChannelEx.Subscription.topic(fn value -> "topic-#{value}" end)
      |> ChannelEx.Subscription.body(fn value -> "body-#{value}" end)

    owner = self()

    id =
      ChannelEx.Subscription.add_publisher(subscription, fn event ->
        send(owner, {:event, event})
      end)

    ChannelEx.Subscription.send(subscription, "x")

    assert_receive {:event, %{topic: "topic-x", body: "body-x"}}, 500

    ChannelEx.Subscription.remove_publisher(subscription, id)
    assert 0 == ChannelEx.Subscription.publisher_count(subscription)
  end

  test "connection state stores and updates arbitrary values" do
    {:ok, state_ref} = ChannelEx.ConnectionState.start_link(%{name: "a"})

    assert %{name: "a"} == ChannelEx.ConnectionState.get(state_ref)
    assert :ok == ChannelEx.ConnectionState.put(state_ref, %{name: "b"})
    assert %{name: "b"} == ChannelEx.ConnectionState.get(state_ref)
    assert :ok == ChannelEx.ConnectionState.update(state_ref, &Map.put(&1, :role, "admin"))
    assert %{name: "b", role: "admin"} == ChannelEx.ConnectionState.get(state_ref)

    assert :ok == ChannelEx.ConnectionState.stop(state_ref)
  end
end
