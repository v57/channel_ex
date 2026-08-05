defmodule ChannelEx.WebSocket do
  @moduledoc false

  @magic "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  def client_handshake_request(host, port, path, headers \\ []) when is_list(headers) do
    key = :crypto.strong_rand_bytes(16) |> Base.encode64()

    request = [
      "GET ",
      if(path in [nil, ""], do: "/", else: path),
      " HTTP/1.1\r\n",
      "host: ",
      host,
      ":",
      Integer.to_string(port),
      "\r\n",
      "content-length: 0\r\n",
      "upgrade: websocket\r\n",
      "sec-websocket-version: 13\r\n",
      "connection: upgrade\r\n",
      "sec-websocket-key: ",
      key,
      "\r\n",
      encode_headers(headers),
      "\r\n"
    ]

    {request, key}
  end

  def server_handshake_response(request_headers) when is_map(request_headers) do
    case Map.get(request_headers, "sec-websocket-key") do
      nil ->
        {:error, :missing_key}

      key ->
        accept = websocket_accept(key)

        {:ok,
         [
           "HTTP/1.1 101 Switching Protocols\r\n",
           "upgrade: websocket\r\n",
           "connection: Upgrade\r\n",
           "sec-websocket-accept: ",
           accept,
           "\r\n",
           "\r\n"
         ]}
    end
  end

  def parse_http_request(buffer) when is_binary(buffer) do
    with {:ok, head, rest} <- split_http_head(buffer),
         [request_line | header_lines] <- String.split(head, "\r\n", trim: true),
         ["GET", request_target, _version] <- String.split(request_line, " ", parts: 3) do
      {path, query} = split_path_query(request_target)

      {:ok,
       %{
         path: path,
         query: decode_query(query),
         headers: decode_headers(header_lines)
       }, rest}
    else
      :more -> :more
      _ -> {:error, :invalid_http_request}
    end
  end

  def parse_http_response(buffer, expected_key)
      when is_binary(buffer) and is_binary(expected_key) do
    with {:ok, head, rest} <- split_http_head(buffer),
         [status_line | header_lines] <- String.split(head, "\r\n", trim: true),
         true <- String.starts_with?(status_line, "HTTP/1.1 101"),
         headers <- decode_headers(header_lines),
         true <- Map.get(headers, "sec-websocket-accept") == websocket_accept(expected_key) do
      {:ok, headers, rest}
    else
      :more -> :more
      false -> {:error, :invalid_http_response}
      _ -> {:error, :invalid_http_response}
    end
  end

  def encode_text(payload, masked? \\ false) when is_binary(payload) do
    encode_frame(payload, 0x1, masked?)
  end

  def encode_pong(payload \\ <<>>, masked? \\ false) when is_binary(payload) do
    encode_frame(payload, 0xA, masked?)
  end

  def encode_close(payload \\ <<>>, masked? \\ false) when is_binary(payload) do
    encode_frame(payload, 0x8, masked?)
  end

  def decode_frames(buffer) when is_binary(buffer) do
    decode_frames(buffer, [])
  end

  defp decode_frames(<<>>, acc), do: {:ok, Enum.reverse(acc), <<>>}

  defp decode_frames(buffer, acc) when byte_size(buffer) < 2 do
    if acc == [] do
      :more
    else
      {:ok, Enum.reverse(acc), buffer}
    end
  end

  defp decode_frames(
         <<fin::1, _rsv::3, opcode::4, masked::1, length_tag::7, rest::binary>> = buffer,
         acc
       ) do
    if fin != 1 do
      {:error, :fragmented_frames_not_supported}
    else
      with {:ok, length, rest} <- decode_length(length_tag, rest),
           {:ok, mask_key, rest} <- decode_mask(masked, rest),
           true <- byte_size(rest) >= length do
        <<payload::binary-size(length), tail::binary>> = rest
        decoded_payload = if mask_key == nil, do: payload, else: apply_mask(payload, mask_key)

        case decode_opcode(opcode, decoded_payload) do
          {:ok, frame} -> decode_frames(tail, [frame | acc])
          error -> error
        end
      else
        :more ->
          if acc == [] do
            :more
          else
            {:ok, Enum.reverse(acc), buffer}
          end

        false ->
          if acc == [] do
            :more
          else
            {:ok, Enum.reverse(acc), buffer}
          end

        error ->
          error
      end
    end
  end

  defp encode_frame(payload, opcode, masked?) do
    length = byte_size(payload)
    {masking_key, masked_payload} = maybe_mask_payload(payload, masked?)

    [
      <<1::1, 0::3, opcode::4>>,
      encode_length(length, masked?),
      masking_key || [],
      masked_payload
    ]
  end

  defp encode_length(length, masked?) when length < 126,
    do: <<if(masked?, do: 1, else: 0)::1, length::7>>

  defp encode_length(length, masked?) when length < 65_536,
    do: <<if(masked?, do: 1, else: 0)::1, 126::7, length::16>>

  defp encode_length(length, masked?),
    do: <<if(masked?, do: 1, else: 0)::1, 127::7, length::64>>

  defp maybe_mask_payload(payload, false), do: {nil, payload}

  defp maybe_mask_payload(payload, true) do
    mask = :crypto.strong_rand_bytes(4)
    {mask, apply_mask(payload, mask)}
  end

  defp decode_length(length, rest) when length < 126, do: {:ok, length, rest}
  defp decode_length(126, <<length::16, rest::binary>>), do: {:ok, length, rest}
  defp decode_length(126, _rest), do: :more
  defp decode_length(127, <<length::64, rest::binary>>), do: {:ok, length, rest}
  defp decode_length(127, _rest), do: :more

  defp decode_mask(0, rest), do: {:ok, nil, rest}
  defp decode_mask(1, <<mask::binary-size(4), rest::binary>>), do: {:ok, mask, rest}
  defp decode_mask(1, _rest), do: :more

  defp decode_opcode(0x1, payload), do: {:ok, {:text, payload}}
  defp decode_opcode(0x8, payload), do: {:ok, {:close, payload}}
  defp decode_opcode(0x9, payload), do: {:ok, {:ping, payload}}
  defp decode_opcode(0xA, payload), do: {:ok, {:pong, payload}}
  defp decode_opcode(_opcode, _payload), do: {:error, :unsupported_opcode}

  defp apply_mask(payload, <<mask::32>> = key) do
    apply_mask(payload, mask, key, [])
  end

  defp apply_mask(<<>>, _mask, _key, acc), do: IO.iodata_to_binary(acc)

  defp apply_mask(<<chunk::32, rest::binary>>, mask, key, acc) do
    apply_mask(rest, mask, key, [acc, <<Bitwise.bxor(chunk, mask)::32>>])
  end

  defp apply_mask(<<a, b, c>>, _mask, <<m0, m1, m2, _m3>>, acc) do
    IO.iodata_to_binary([acc, <<Bitwise.bxor(a, m0), Bitwise.bxor(b, m1), Bitwise.bxor(c, m2)>>])
  end

  defp apply_mask(<<a, b>>, _mask, <<m0, m1, _m2, _m3>>, acc) do
    IO.iodata_to_binary([acc, <<Bitwise.bxor(a, m0), Bitwise.bxor(b, m1)>>])
  end

  defp apply_mask(<<a>>, _mask, <<m0, _m1, _m2, _m3>>, acc) do
    IO.iodata_to_binary([acc, <<Bitwise.bxor(a, m0)>>])
  end

  defp split_http_head(buffer) do
    case :binary.match(buffer, "\r\n\r\n") do
      {index, 4} ->
        head = binary_part(buffer, 0, index)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, head, rest}

      :nomatch ->
        :more
    end
  end

  defp split_path_query(request_target) do
    case String.split(request_target, "?", parts: 2) do
      [path, query] -> {path, query}
      [path] -> {path, nil}
    end
  end

  defp decode_query(nil), do: %{}
  defp decode_query(""), do: %{}

  defp decode_query(query) do
    query
    |> URI.query_decoder()
    |> Enum.into(%{})
  end

  defp decode_headers(lines) do
    Enum.reduce(lines, %{}, fn line, acc ->
      case String.split(line, ":", parts: 2) do
        [name, value] ->
          Map.put(acc, String.downcase(String.trim(name)), String.trim(value))

        _ ->
          acc
      end
    end)
  end

  defp encode_headers(headers) do
    Enum.reduce(headers, [], fn {name, value}, acc ->
      [acc, to_string(name), ": ", to_string(value), "\r\n"]
    end)
  end

  defp websocket_accept(key) do
    :crypto.hash(:sha, key <> @magic)
    |> Base.encode64()
  end
end
