defmodule ChannelEx.HostileInputTest do
  use ExUnit.Case, async: false

  @moduletag capture_log: true

  alias ChannelEx.WebSocket

  setup do
    channel =
      ChannelEx.new()
      |> ChannelEx.post("echo", fn %{body: body} -> body end)
      |> ChannelEx.post("strict", fn %{body: %{"x" => x}} -> x end)
      |> ChannelEx.post("fail", fn _ -> raise "unauthorized" end)
      |> ChannelEx.post("close", fn %{sender: sender} -> ChannelEx.Sender.stop(sender) end)

    listener = ChannelEx.listen(channel, 0)
    on_exit(fn -> ChannelEx.ServerListener.stop(listener) end)
    %{listener: listener, port: listener.port}
  end

  defp raw_connect(port) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    {request, key} = WebSocket.client_handshake_request("127.0.0.1", port, "/")
    :ok = :gen_tcp.send(socket, request)
    {:ok, _headers, <<>>} = recv_http(socket, key, <<>>)
    socket
  end

  defp recv_http(socket, key, buffer) do
    case WebSocket.parse_http_response(buffer, key) do
      :more ->
        {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)
        recv_http(socket, key, buffer <> data)

      other ->
        other
    end
  end

  defp masked_frame(opcode, payload, fin \\ true) do
    <<_::4, _::4, rest::binary>> = IO.iodata_to_binary(WebSocket.encode_text(payload, true))
    <<if(fin, do: 1, else: 0)::1, 0::3, opcode::4, rest::binary>>
  end

  defp recv_text(socket) do
    {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)
    {:ok, [{:text, payload}], <<>>} = WebSocket.decode_frames(data)
    Jason.decode!(payload)
  end

  defp assert_alive(port) do
    socket = raw_connect(port)
    :ok = :gen_tcp.send(socket, masked_frame(1, ~s({"id":1,"path":"echo","body":"ok"})))
    assert %{"id" => 1, "body" => "ok"} = recv_text(socket)
    :gen_tcp.close(socket)
  end

  test "malformed messages never take the server down", %{port: port} do
    deep = String.duplicate("[", 100_000) <> String.duplicate("]", 100_000)

    for payload <- ["{", "null", "123", ~s("str"), "[null, 1]", deep, <<0xFF, 0xFE>>] do
      socket = raw_connect(port)
      :ok = :gen_tcp.send(socket, masked_frame(1, payload))
      # The bad message is dropped (TS parity) and the connection stays usable.
      :ok = :gen_tcp.send(socket, masked_frame(1, ~s({"id":2,"path":"echo","body":1})))
      assert %{"id" => 2, "body" => 1} = recv_text(socket)
      :gen_tcp.close(socket)
    end

    assert_alive(port)
  end

  test "oversized frames and handshakes close only that connection", %{port: port} do
    for length <- [0x7FFF_FFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF, 1_073_741_824] do
      socket = raw_connect(port)
      :ok = :gen_tcp.send(socket, <<0x81, 0xFF, length::64, 1, 2, 3, 4>>)
      assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)
    end

    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false])
    :ok = :gen_tcp.send(socket, "GET / HTTP/1.1\r\nx: " <> String.duplicate("a", 70_000))
    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 2_000)

    assert_alive(port)
  end

  test "fragmented messages are reassembled with control frames in between", %{port: port} do
    socket = raw_connect(port)
    message = ~s({"id":3,"path":"echo","body":"fragmented"})
    {a, rest} = String.split_at(message, 10)
    {b, c} = String.split_at(rest, 10)

    :ok =
      :gen_tcp.send(socket, [
        masked_frame(1, a, false),
        masked_frame(0x9, "p"),
        masked_frame(0, b, false),
        masked_frame(0, c, true)
      ])

    {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)

    data =
      case WebSocket.decode_frames(data) do
        {:ok, [{:pong, "p"}], <<>>} ->
          {:ok, more} = :gen_tcp.recv(socket, 0, 2_000)
          more

        {:ok, [{:pong, "p"}, {:text, _} = text], <<>>} ->
          text

        other ->
          flunk("unexpected frames: #{inspect(other)}")
      end

    payload =
      case data do
        {:text, payload} -> payload
        bin -> elem(hd(elem(WebSocket.decode_frames(bin), 1)), 1)
      end

    assert %{"id" => 3, "body" => "fragmented"} = Jason.decode!(payload)
  end

  test "internal handler errors are not leaked to clients", %{port: port} do
    client = ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
    assert {:error, "internal error"} = ChannelEx.ClientSender.send(client, "strict", 1)
    assert {:error, "unauthorized"} = ChannelEx.ClientSender.send(client, "fail")
    assert {:ok, "x"} = ChannelEx.ClientSender.send(client, "strict", %{"x" => "x"})
    ChannelEx.ClientSender.stop(client)
  end

  test "server-side sender stop closes only that connection", %{port: port} do
    other = ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
    victim = raw_connect(port)
    :ok = :gen_tcp.send(victim, masked_frame(1, ~s({"id":1,"path":"close"})))
    assert {:error, :closed} = recv_until_closed(victim)
    assert {:ok, "still"} = ChannelEx.ClientSender.send(other, "echo", "still")
    ChannelEx.ClientSender.stop(other)
  end

  defp recv_until_closed(socket) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, _data} -> recv_until_closed(socket)
      error -> error
    end
  end

  describe "client" do
    defp fake_server(reply_fun) do
      {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
      {:ok, port} = :inet.port(listener)

      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, request} = :gen_tcp.recv(socket, 0, 2_000)
        reply_fun.(socket, request)
        Process.sleep(1_000)
      end)

      port
    end

    defp accept_reply(request) do
      [_, key] = Regex.run(~r/sec-websocket-key: (\S+)/i, request)

      accept =
        :crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11") |> Base.encode64()

      "HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: #{accept}\r\n\r\n"
    end

    test "connect returns an error on a non-101 handshake reply" do
      port =
        fake_server(fn socket, _request ->
          :gen_tcp.send(socket, "HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n")
        end)

      assert {:error, {:handshake_failed, :invalid_http_response}} =
               ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
    end

    test "connect returns an error when the server never answers" do
      port = fake_server(fn _socket, _request -> :ok end)

      assert {:error, {:handshake_failed, :timeout}} =
               ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}", connect_timeout: 200)
    end

    test "connect returns an error when nothing listens" do
      {:ok, listener} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(listener)
      :gen_tcp.close(listener)

      assert {:error, :econnrefused} =
               ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
    end

    test "a huge frame length closes the client and settles pending calls" do
      parent = self()

      port =
        fake_server(fn socket, request ->
          :gen_tcp.send(socket, accept_reply(request))
          {:ok, _call} = :gen_tcp.recv(socket, 0, 2_000)
          send(parent, :call_received)
          :gen_tcp.send(socket, <<0x81, 0x7F, 0x7FFF_FFFF_FFFF_FFFF::64>>)
        end)

      client = ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
      Process.unlink(client.ws)
      ref = Process.monitor(client.ws)
      assert {:error, "disconnected"} = ChannelEx.ClientSender.send(client, "echo", 1, nil, 2_000)
      assert_receive :call_received
      assert_receive {:DOWN, ^ref, :process, _, :normal}, 2_000
    end

    test "an undecodable reply settles pending calls instead of hanging" do
      port =
        fake_server(fn socket, request ->
          :gen_tcp.send(socket, accept_reply(request))
          {:ok, _call} = :gen_tcp.recv(socket, 0, 2_000)
          :gen_tcp.send(socket, <<0x81, 1, "{">>)
        end)

      client = ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
      Process.unlink(client.ws)
      assert {:error, "disconnected"} = ChannelEx.ClientSender.send(client, "echo", 1, nil, 2_000)
    end

    test "send on a failed connect returns the connect error" do
      {:ok, listener} = :gen_tcp.listen(0, [])
      {:ok, port} = :inet.port(listener)
      :gen_tcp.close(listener)
      client = ChannelEx.connect(ChannelEx.new(), port)
      assert {:error, :econnrefused} = ChannelEx.send(client, "echo", 1)
    end

    test "fragmented replies are reassembled" do
      port =
        fake_server(fn socket, request ->
          :gen_tcp.send(socket, accept_reply(request))
          {:ok, _call} = :gen_tcp.recv(socket, 0, 2_000)
          reply = ~s({"id":0,"body":"joined"})
          {a, b} = String.split_at(reply, 8)
          :gen_tcp.send(socket, <<0x01, byte_size(a)>> <> a)
          :gen_tcp.send(socket, <<0x89, 0>>)
          :gen_tcp.send(socket, <<0x80, byte_size(b)>> <> b)
        end)

      client = ChannelEx.connect(ChannelEx.new(), "ws://127.0.0.1:#{port}")
      assert {:ok, "joined"} = ChannelEx.ClientSender.send(client, "echo", 1, nil, 2_000)
      ChannelEx.ClientSender.stop(client)
    end
  end

  describe "codec" do
    test "declared lengths above the cap are rejected before buffering" do
      for length <- [0x7FFF_FFFF_FFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF, 16 * 1024 * 1024 + 1] do
        assert {:error, :frame_too_large} = WebSocket.decode_frames(<<0x81, 0x7F, length::64>>)
      end

      assert {:error, :frame_too_large} = WebSocket.decode_frames(<<0x81, 5, "hello">>, 4)
      assert {:ok, [{:text, "hello"}], <<>>} = WebSocket.decode_frames(<<0x81, 5, "hello">>, 5)
    end

    test "reassembly enforces the message cap and continuation order" do
      frames = [{:fragment, 1, false, "abc"}, {:fragment, 0, true, "de"}]
      assert {:error, :message_too_large} = WebSocket.reassemble(frames, nil, 4)
      assert {:ok, [{:text, "abcde"}], nil} = WebSocket.reassemble(frames, nil, 5)

      assert {:error, :unexpected_continuation} =
               WebSocket.reassemble([{:fragment, 0, true, "x"}], nil)

      assert {:ok, [], {1, _, 3}} = WebSocket.reassemble([{:fragment, 1, false, "abc"}], nil)
    end

    test "handshake heads are capped" do
      huge = "GET / HTTP/1.1\r\nx: " <> String.duplicate("a", 70_000)
      assert {:error, :handshake_too_large} = WebSocket.parse_http_request(huge)
      assert {:error, :handshake_too_large} = WebSocket.parse_http_response(huge, "k")
    end
  end
end
