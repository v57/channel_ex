defmodule ChannelEx.WebSocket do
  @moduledoc false

  @magic "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
  @default_max_payload 16 * 1024 * 1024
  @max_handshake_size 64 * 1024

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
      {:error, :handshake_too_large} = error -> error
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
      {:error, :handshake_too_large} = error -> error
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

  @doc """
  Largest accepted frame payload and reassembled message size (Bun's default maxPayloadLength).
  """
  def default_max_payload, do: @default_max_payload

  @doc "Largest accepted HTTP upgrade request/response head."
  def max_handshake_size, do: @max_handshake_size

  @doc """
  Decodes every complete frame in `buffer`.

  Returns `{:ok, frames, rest}`, `:more` (no complete frame yet) or `{:error, reason}`.
  Complete data frames come out as `{:text, payload}` / `{:binary, payload}`; pieces of a
  fragmented message come out as `{:fragment, opcode, fin?, payload}` (opcode 0 for
  continuation frames) and are joined by `reassemble/3`. A declared length above
  `max_payload` is an error before anything is buffered.
  """
  def decode_frames(buffer, max_payload \\ @default_max_payload) when is_binary(buffer) do
    decode_frames(buffer, max_payload, [])
  end

  defp decode_frames(buffer, max_payload, acc) do
    case decode_frame(buffer, max_payload) do
      {:ok, frame, tail} -> decode_frames(tail, max_payload, [frame | acc])
      :more when acc == [] -> :more
      :more -> {:ok, Enum.reverse(acc), buffer}
      {:error, _reason} = error -> error
    end
  end

  defp decode_frame(
         <<fin::1, rsv::3, opcode::4, masked::1, length_tag::7, rest::binary>>,
         max_payload
       ) do
    with :ok <- check_header(fin, rsv, opcode, length_tag),
         {:ok, length, rest} <- decode_length(length_tag, rest),
         :ok <- check_length(length, max_payload),
         {:ok, mask_key, rest} <- decode_mask(masked, rest),
         true <- byte_size(rest) >= length || :more do
      payload = binary_part(rest, 0, length)
      tail = binary_part(rest, length, byte_size(rest) - length)
      payload = if mask_key == nil, do: payload, else: apply_mask(payload, mask_key)
      {:ok, frame(opcode, fin == 1, payload), tail}
    end
  end

  defp decode_frame(_buffer, _max_payload), do: :more

  defp check_header(_fin, rsv, _opcode, _length_tag) when rsv != 0,
    do: {:error, :reserved_bits_set}

  defp check_header(fin, _rsv, opcode, length_tag) when opcode >= 0x8 do
    cond do
      opcode not in [0x8, 0x9, 0xA] -> {:error, :unsupported_opcode}
      fin != 1 -> {:error, :fragmented_control_frame}
      length_tag > 125 -> {:error, :control_frame_too_large}
      true -> :ok
    end
  end

  defp check_header(_fin, _rsv, opcode, _length_tag) when opcode in [0x0, 0x1, 0x2], do: :ok
  defp check_header(_fin, _rsv, _opcode, _length_tag), do: {:error, :unsupported_opcode}

  defp check_length(length, max_payload) when length > max_payload, do: {:error, :frame_too_large}
  defp check_length(_length, _max_payload), do: :ok

  defp frame(0x1, true, payload), do: {:text, payload}
  defp frame(0x2, true, payload), do: {:binary, payload}
  defp frame(0x8, _fin, payload), do: {:close, payload}
  defp frame(0x9, _fin, payload), do: {:ping, payload}
  defp frame(0xA, _fin, payload), do: {:pong, payload}
  defp frame(opcode, fin, payload), do: {:fragment, opcode, fin, payload}

  @doc """
  Joins fragmented messages (RFC 6455 section 5.4). `partial` is `nil` or the state returned
  by the previous call. Control frames may arrive between fragments and pass straight through.
  Returns `{:ok, frames, partial}` or `{:error, reason}`.
  """
  def reassemble(frames, partial, max_payload \\ @default_max_payload) do
    reassemble(frames, partial, max_payload, [])
  end

  defp reassemble([], partial, _max, acc), do: {:ok, Enum.reverse(acc), partial}

  defp reassemble([{:fragment, opcode, fin, payload} | rest], nil, max, acc)
       when opcode in [0x1, 0x2] do
    continue_message({opcode, [payload], byte_size(payload)}, fin, rest, max, acc)
  end

  defp reassemble([{:fragment, 0x0, fin, payload} | rest], {opcode, parts, size}, max, acc) do
    continue_message({opcode, [parts, payload], size + byte_size(payload)}, fin, rest, max, acc)
  end

  defp reassemble([{:fragment, _opcode, _fin, _payload} | _rest], _partial, _max, _acc),
    do: {:error, :unexpected_continuation}

  defp reassemble([{kind, _payload} | _rest], {_, _, _}, _max, _acc)
       when kind in [:text, :binary],
       do: {:error, :interleaved_data_frame}

  defp reassemble([frame | rest], partial, max, acc),
    do: reassemble(rest, partial, max, [frame | acc])

  defp continue_message({_opcode, _parts, size}, _fin, _rest, max, _acc) when size > max,
    do: {:error, :message_too_large}

  defp continue_message({opcode, parts, _size} = partial, fin, rest, max, acc) do
    if fin do
      reassemble(rest, nil, max, [frame(opcode, true, IO.iodata_to_binary(parts)) | acc])
    else
      reassemble(rest, partial, max, acc)
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
      {index, 4} when index + 4 > @max_handshake_size ->
        {:error, :handshake_too_large}

      {index, 4} ->
        head = binary_part(buffer, 0, index)
        rest = binary_part(buffer, index + 4, byte_size(buffer) - index - 4)
        {:ok, head, rest}

      :nomatch when byte_size(buffer) > @max_handshake_size ->
        {:error, :handshake_too_large}

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
