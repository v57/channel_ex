if ChannelEx.Quic.available?() do
  defmodule ChannelEx.QuicTest do
    # The cases of channel-go/quic/quic_test.go. The port's WebSocket client doesn't reconnect, so
    # neither does the QUIC one: a dropped connection fails what waits, as over WebSocket.
    use ExUnit.Case, async: false

    alias ChannelEx.Quic.Frame

    # The TypeScript reference's test certificate: localhost, 127.0.0.1 and ::1
    @cert_dir Path.expand("../../channel/tests/quic", __DIR__)
    @fingerprint "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B"

    setup_all do
      {:ok,
       cert: File.read!(Path.join(@cert_dir, "cert.pem")),
       key: File.read!(Path.join(@cert_dir, "key.pem"))}
    end

    defp listen(context, options \\ []) do
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
        # Closes the connection before the reply goes out (Sender.stop/1 would run after it)
        |> ChannelEx.post("drop", fn %{sender: %{transport: {:server_quic, conn}}} ->
          :quicer.async_shutdown_connection(conn, 0, 0)
          nil
        end)
        |> ChannelEx.post("tick", fn _ ->
          ChannelEx.Subscription.send(ticks, "a", "tick")
          nil
        end)
        |> ChannelEx.stream("count", fn _ -> [0, 1, 2] end)
        |> ChannelEx.events(%{ticks: ticks})

      listener =
        ChannelEx.listen_quic(
          channel,
          Keyword.get(options, :address, 0),
          key: context.key,
          cert: context.cert,
          state: fn headers -> headers end,
          on_connect: fn %{sender: sender} -> send(test, {:connected, sender}) end,
          on_disconnect: fn _ -> send(test, :disconnected) end
        )

      on_exit(fn -> ChannelEx.ServerListener.stop(listener) end)
      %{listener: listener, ticks: ticks}
    end

    defp connect(address, options) do
      client_channel = ChannelEx.new() |> ChannelEx.post("hello", fn _ -> "client world" end)

      case ChannelEx.connect(client_channel, address, options) do
        %ChannelEx.ClientSender{} = client ->
          on_exit(fn -> ChannelEx.ClientSender.stop(client) end)
          client

        error ->
          error
      end
    end

    defp address(%{listener: listener}), do: "quic://127.0.0.1:#{listener.port}"

    test "frames go through the stream both ways", context do
      server = listen(context)

      client =
        connect(address(server),
          quic: [ca: context.cert],
          headers: fn -> %{"X-Name" => "elixir"} end
        )

      assert {:ok, "world"} = ChannelEx.send(client, "hello")
      body = %{"list" => [1, "two", true], "text" => String.duplicate("é", 20_000)}
      assert {:ok, ^body} = ChannelEx.send(client, "echo", body)
      assert {:ok, nil} = ChannelEx.send(client, "nil")

      # hello headers become the state, names lowercased
      assert {:ok, "elixir"} = ChannelEx.send(client, "name")

      # the server calls the client back
      assert_receive {:connected, server_sender}
      assert {:ok, "client world"} = ChannelEx.Sender.send(server_sender, "hello")

      assert [0, 1, 2] = client |> ChannelEx.values("count") |> Enum.to_list()

      # events
      test = self()

      {:ok, subscription} =
        ChannelEx.ClientSender.subscribe(client, "ticks", "a", &send(test, {:event, &1}))

      ChannelEx.Subscription.send(server.ticks, "a", "tick")
      ChannelEx.Subscription.send(server.ticks, "b", "not for us")
      ChannelEx.Subscription.send(server.ticks, "a", "tick")
      assert_receive {:event, "tick"}, 1_000
      assert_receive {:event, "tick"}, 1_000
      subscription.cancel.()
      Process.sleep(20)
      ChannelEx.Subscription.send(server.ticks, "a", "after cancel")
      refute_receive {:event, _}, 100

      # many in flight
      requests = for i <- 0..1999, do: ChannelEx.ClientSender.request(client, "echo", i)

      for {request, i} <- Enum.with_index(requests) do
        assert {:ok, ^i} = ChannelEx.ClientSender.await(request)
      end

      assert %{connected: true, pending: 0} = ChannelEx.Client.Socket.status(client.ws)
    end

    test "each client dials from its own UDP socket", context do
      server = listen(context)
      one = connect(address(server), quic: [ca: context.cert])
      two = connect(address(server), quic: [ca: context.cert])
      assert {:ok, {_, port_one}} = GenServer.call(one.ws, :sockname)
      assert {:ok, {_, port_two}} = GenServer.call(two.ws, :sockname)
      assert port_one != port_two
      assert {:ok, "world"} = ChannelEx.send(one, "hello")
      assert {:ok, "world"} = ChannelEx.send(two, "hello")
    end

    test "certificate trust", context do
      server = listen(context)
      address = address(server)

      pin = @fingerprint |> String.replace(":", "") |> String.downcase()

      assert {:ok, "world"} =
               address |> connect(quic: [fingerprint: pin]) |> ChannelEx.send("hello")

      assert {:ok, "world"} =
               address |> connect(quic: [fingerprint: @fingerprint]) |> ChannelEx.send("hello")

      assert {:ok, "world"} =
               address
               |> connect(quic: [ca: context.cert, servername: "localhost"])
               |> ChannelEx.send("hello")

      assert {:ok, "world"} =
               "quic://localhost:#{server.listener.port}"
               |> connect(quic: [ca: context.cert])
               |> ChannelEx.send("hello")

      for {name, quic} <- [
            {"system roots", []},
            {"wrong pin", [fingerprint: String.duplicate("00", 32)]},
            {"name not in cert", [ca: context.cert, servername: "example.com"]}
          ] do
        assert {:error, _} = connect(address, quic: quic), name
      end

      assert {:error, {:invalid_fingerprint, "nope"}} =
               connect(address, quic: [fingerprint: "nope"])

      assert {:error, {:invalid_address, _}} =
               ChannelEx.Quic.Client.connect(ChannelEx.new(), "ws://127.0.0.1:1", [])
    end

    test "an IP address must be in the certificate", context do
      other =
        with {:ok, interfaces} <- :inet.getifaddrs() do
          for {_name, options} <- interfaces,
              {:addr, {a, _, _, _} = ip} <- options,
              a != 127,
              do: ip
        end

      case other do
        [ip | _] ->
          server = listen(context, address: "0.0.0.0:0")
          address = "quic://#{:inet.ntoa(ip)}:#{server.listener.port}"

          assert {:error, {:transport_down, %{status: :bad_certificate}}} =
                   connect(address, quic: [ca: context.cert])

          # The name, or a pin, still reach it
          assert %ChannelEx.ClientSender{} =
                   connect(address, quic: [ca: context.cert, servername: "localhost"])

          assert %ChannelEx.ClientSender{} = connect(address, quic: [fingerprint: @fingerprint])

        _ ->
          IO.puts("skipped: no IPv4 address besides 127.0.0.1")
      end
    end

    # A raw session without the Channel client, to check the wire rules
    defp raw_session(%{listener: listener}) do
      {:ok, conn} =
        :quicer.connect(
          ~c"127.0.0.1",
          listener.port,
          %{alpn: [~c"channel/1"], verify: :none, idle_timeout_ms: 10_000},
          5_000
        )

      {:ok, stream} = :quicer.start_stream(conn, %{active: true})
      on_exit(fn -> :quicer.async_shutdown_connection(conn, 0, 0) end)
      {conn, stream}
    end

    defp read_frames(stream, count, buffer \\ <<>>, frames \\ []) do
      if length(frames) >= count do
        frames
      else
        receive do
          {:quic, data, ^stream, _props} when is_binary(data) ->
            {:ok, new, rest} = Frame.decode(buffer <> data)
            read_frames(stream, count, rest, frames ++ new)
        after
          2_000 -> frames
        end
      end
    end

    defp closed?(conn) do
      receive do
        {:quic, event, ^conn, _props} when event in [:shutdown, :transport_shutdown, :closed] ->
          true
      after
        2_000 -> false
      end
    end

    defp raw_send(stream, frames) do
      {:ok, _} = :quicer.async_send(stream, frames)
    end

    test "raw frames: hello, a keepalive, then a batch", context do
      server = listen(context)
      {_conn, stream} = raw_session(server)
      hello = ChannelEx.JSON.encode!(%{"hello" => %{"X-Name" => "raw", "ignored" => 1}})

      raw_send(stream, [
        Frame.encode(hello),
        Frame.keepalive(),
        Frame.encode(~s([{"id":1,"path":"name"},{"id":2,"path":"hello"}]))
      ])

      responses =
        stream
        |> read_frames(1)
        |> Enum.flat_map(&ChannelEx.JSON.decode_any/1)
        |> Map.new(&{&1["id"], &1["body"]})

      assert responses == %{1 => "raw", 2 => "world"}
    end

    test "raw frames: a nil response and a nil snapshot go out as null", context do
      server = listen(context)
      {_conn, stream} = raw_session(server)

      raw_send(stream, [
        Frame.encode(~s({"hello":{}})),
        Frame.encode(~s({"id":1,"path":"nil"})),
        Frame.encode(~s({"id":2,"sub":"ticks","body":"a"}))
      ])

      frames = read_frames(stream, 2)
      assert ~s({"id":1,"body":null}) in frames
      assert ~s({"id":2,"topic":"ticks/a","body":null}) in frames

      assert [%{id: 1, body: nil}, %{id: 2, topic: "ticks/a", body: nil}] =
               frames |> Enum.flat_map(&ChannelEx.Wire.decode_any/1) |> Enum.sort_by(& &1.id)
    end

    test "one stream per connection", context do
      server = listen(context)
      {conn, stream} = raw_session(server)
      raw_send(stream, Frame.encode(~s({"hello":{}})))
      assert_receive {:connected, _}, 2_000

      # The server allows one bidirectional stream and no unidirectional one, so starting a
      # second stream or a unidirectional one is refused (flags 0 and
      # QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL; start_flag IMMEDIATE | FAIL_BLOCKED, and the event
      # mask reports the start)
      for open_flag <- [0, 1] do
        {:ok, extra} =
          :quicer.start_stream(conn, %{
            active: true,
            open_flag: open_flag,
            start_flag: 3,
            quic_event_mask: 1
          })

        assert_receive {:quic, :start_completed, ^extra, %{status: :stream_limit_reached}}, 2_000
      end

      # The first one still works
      raw_send(stream, Frame.encode(~s({"id":7,"path":"hello"})))
      assert [frame] = read_frames(stream, 1)
      assert %{"id" => 7, "body" => "world"} = ChannelEx.JSON.decode!(frame)
    end

    for {name, frames} <- [
          {"a first frame that isn't a hello closes",
           quote(do: Frame.encode(~s({"id":1,"path":"hello"})))},
          {"invalid JSON closes",
           quote(do: [Frame.encode(~s({"hello":{}})), Frame.encode("{nope")])},
          {"a frame over the limit closes",
           quote(do: [Frame.encode(~s({"hello":{}})), <<0x81, 0, 0, 1>>])}
        ] do
      test name, context do
        server = listen(context)
        {conn, stream} = raw_session(server)
        raw_send(stream, unquote(frames))
        assert closed?(conn)
      end
    end

    test "a server drop fails what waits, like the WebSocket client", context do
      server = listen(context)
      client = connect(address(server), quic: [ca: context.cert])
      assert {:ok, "world"} = ChannelEx.send(client, "hello")
      ref = Process.monitor(client.ws)
      assert {:error, "disconnected"} = ChannelEx.send(client, "drop", nil, nil, 2_000)
      assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
      assert_receive :disconnected, 2_000
    end

    test "stopping the client fails what still waits and closes the server side", context do
      server = listen(context)
      client = connect(address(server), quic: [ca: context.cert])
      request = ChannelEx.ClientSender.request(client, "slow")
      Process.sleep(50)
      ChannelEx.ClientSender.stop(client)
      assert {:error, "disconnected"} = ChannelEx.ClientSender.await(request, 2_000)
      assert_receive :disconnected, 2_000
    end

    test "a server that isn't there fails the connect", context do
      assert {:error, _} =
               connect("quic://127.0.0.1:1", quic: [ca: context.cert], connect_timeout: 1_000)
    end

    test "stopping the listener closes its connections", context do
      server = listen(context)
      client = connect(address(server), quic: [ca: context.cert])
      ref = Process.monitor(client.ws)
      ChannelEx.ServerListener.stop(server.listener)
      assert_receive :disconnected, 2_000
      assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
    end

    # The TypeScript reference's fixture (channel/tests/server.ts) serves QUIC on UDP next to its
    # WebSocket on TCP, on CHANNEL_TEST_PORT (2104 here, so it doesn't meet the 2049 suites).
    describe "the TypeScript reference" do
      @describetag :interop

      test "server answers this client", context do
        port = String.to_integer(System.get_env("CHANNEL_TEST_PORT", "2104"))
        fixture = ChannelEx.Test.TSFixture.start!(port)
        on_exit(fn -> ChannelEx.Test.TSFixture.stop(fixture) end)
        names = ChannelEx.Subscription.new() |> ChannelEx.Subscription.set_body(& &1)

        channel =
          ChannelEx.new()
          |> ChannelEx.post("hello", fn _ -> "client world" end)
          |> ChannelEx.stream("stream/values", fn _ -> [0, 1, 2] end)
          |> ChannelEx.events(%{names: names})

        # The QUIC listener starts right after the WebSocket one
        client =
          Enum.find_value(1..20, fn _ ->
            case ChannelEx.connect(channel, "quic://127.0.0.1:#{port}",
                   quic: [ca: context.cert],
                   headers: fn -> %{"X-Port" => "elixir"} end
                 ) do
              %ChannelEx.ClientSender{} = client ->
                client

              {:error, _} ->
                Process.sleep(100)
                nil
            end
          end) || flunk("the fixture on #{port} doesn't serve QUIC")

        on_exit(fn -> ChannelEx.ClientSender.stop(client) end)

        assert {:ok, "world"} = ChannelEx.send(client, "hello")
        assert {:ok, %{"a" => [1, "b"]}} = ChannelEx.send(client, "echo", %{"a" => [1, "b"]})
        assert {:ok, "client world"} = ChannelEx.send(client, "mirror")
        assert {:ok, "quic"} = ChannelEx.send(client, "auth", %{"name" => "quic"})
        assert {:ok, "quic"} = ChannelEx.send(client, "auth/name")
        assert {:ok, %{"x-port" => "elixir"}} = ChannelEx.send(client, "v2/headers")
        assert [0, 1, 2] = client |> ChannelEx.values("mirror/stream") |> Enum.to_list()

        # The fixture subscribes to this client's names/1 and cancels after the first event
        assert {:ok, nil} = ChannelEx.send(client, "mirror/events")
        Process.sleep(20)
        ChannelEx.Subscription.send(names, "1", "h")
        assert {:ok, "world"} = ChannelEx.send(client, "hello")

        assert {:error, _} = ChannelEx.send(client, "v2/error/code")
        assert {:error, "api not found"} = ChannelEx.send(client, "notfound")

        # The fixture drops the connection without answering
        assert {:ok, nil} = ChannelEx.send(client, "v2/drops/reset")
        ref = Process.monitor(client.ws)
        assert {:error, "disconnected"} = ChannelEx.send(client, "v2/drop", nil, nil, 2_000)
        assert_receive {:DOWN, ^ref, :process, _, _}, 2_000
      end

      test "client calls this server", context do
        server = listen(context)
        test = self()

        task =
          Task.async(fn ->
            ChannelEx.Test.TSFixture.run_client(
              address(server),
              Path.join(@cert_dir, "cert.pem")
            )
          end)

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
  end
end
