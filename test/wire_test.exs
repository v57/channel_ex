defmodule ChannelEx.WireTest do
  use ExUnit.Case, async: true

  test "wire codec fast path decodes compact response envelopes" do
    assert [%{id: 1, body: 5}] = ChannelEx.Wire.decode_any(~s({"id":1,"body":5}))
    assert [%{id: 2, done: true}] = ChannelEx.Wire.decode_any(~s({"id":2,"done":true}))
    assert [%{id: 3, error: "boom"}] = ChannelEx.Wire.decode_any(~s({"id":3,"error":"boom"}))
  end

  test "wire codec fast path decodes compact request envelopes" do
    assert [%{id: 4, path: "text/count", body: %{"text" => "hello"}}] =
             ChannelEx.Wire.decode_any(~s({"id":4,"path":"text/count","body":{"text":"hello"}}))

    assert [
             %{id: 5, path: "text/count", body: %{"text" => "hello"}},
             %{id: 6, path: "text/count", body: %{"text" => "hello"}}
           ] =
             ChannelEx.Wire.decode_any(
               ~s([{"id":5,"path":"text/count","body":{"text":"hello"}},{"id":6,"path":"text/count","body":{"text":"hello"}}])
             )
  end

  test "wire codec encodes batched payloads in channel field order" do
    payload =
      ChannelEx.Wire.encode!([
        %{id: 1, path: "hello", body: %{name: "dimas"}},
        %{id: 2, done: true}
      ])
      |> IO.iodata_to_binary()

    assert payload ==
             ~s([{"id":1,"path":"hello","body":{"name":"dimas"}},{"id":2,"done":true}])
  end

  test "nil goes out as null and null comes back as nil" do
    assert ~s({"id":1,"body":null}) ==
             IO.iodata_to_binary(ChannelEx.Wire.encode!([%{id: 1, body: nil}]))

    assert ~s({"id":2,"topic":"ticks/a","body":null}) ==
             IO.iodata_to_binary(ChannelEx.Wire.encode!(%{id: 2, topic: "ticks/a", body: nil}))

    assert ~s({"a":[null]}) == ChannelEx.JSON.encode!(%{"a" => [nil]})

    # The fast path, then the JSON library for what it doesn't parse (a float, keys out of order)
    assert [%{id: 3, body: nil}] = ChannelEx.Wire.decode_any(~s({"id":3,"body":null}))

    assert [%{"id" => 4, "body" => %{"a" => nil, "b" => 1.5}}] =
             ChannelEx.Wire.decode_any(~s({"id":4,"body":{"a":null,"b":1.5}}))

    assert [%{"body" => nil, "id" => 5}] = ChannelEx.Wire.decode_any(~s({"body":null,"id":5}))
    assert nil == ChannelEx.JSON.decode!("null")
  end

  test "websocket codec round trips masked and unmasked text frames" do
    encoded =
      "hello"
      |> ChannelEx.WebSocket.encode_text(true)
      |> IO.iodata_to_binary()

    assert {:ok, [{:text, "hello"}], <<>>} = ChannelEx.WebSocket.decode_frames(encoded)

    encoded =
      "world"
      |> ChannelEx.WebSocket.encode_text(false)
      |> IO.iodata_to_binary()

    assert {:ok, [{:text, "world"}], <<>>} = ChannelEx.WebSocket.decode_frames(encoded)
  end

  test "websocket http handshake helpers parse request and response" do
    {request, key} =
      ChannelEx.WebSocket.client_handshake_request("127.0.0.1", 2049, "/ws?name=test", [
        {"x-token", "a"}
      ])

    request = IO.iodata_to_binary(request)

    assert {:ok, parsed_request, <<>>} = ChannelEx.WebSocket.parse_http_request(request)
    assert parsed_request.path == "/ws"
    assert parsed_request.query == %{"name" => "test"}
    assert parsed_request.headers["x-token"] == "a"

    assert {:ok, response} = ChannelEx.WebSocket.server_handshake_response(parsed_request.headers)
    response = IO.iodata_to_binary(response)

    assert {:ok, headers, <<>>} = ChannelEx.WebSocket.parse_http_response(response, key)
    assert is_binary(headers["sec-websocket-accept"])
  end
end
