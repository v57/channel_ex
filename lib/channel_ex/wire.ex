defmodule ChannelEx.Wire do
  @moduledoc false

  def encode!(payload) when is_list(payload) do
    case payload do
      [single] -> encode_one!(single)
      many -> ["[", join_encoded(many), "]"]
    end
  end

  def encode!(payload) when is_map(payload), do: encode_one!(payload)

  def decode_any(payload) when is_binary(payload) do
    case fast_decode(payload) do
      {:ok, decoded} -> decoded
      :error -> ChannelEx.JSON.decode_any(payload)
    end
  end

  @doc """
  Decodes a frame payload into a list of message maps without raising.

  Invalid JSON (or invalid UTF-8, too-deep nesting, ...) returns `:error`; non-object
  elements such as `null`, `123` or `"str"` are dropped.
  """
  def decode_messages(payload) when is_binary(payload) do
    decoded =
      try do
        {:ok, decode_any(payload)}
      rescue
        _ -> :error
      catch
        _, _ -> :error
      end

    case decoded do
      {:ok, messages} when is_list(messages) -> {:ok, Enum.filter(messages, &is_map/1)}
      {:ok, message} when is_map(message) -> {:ok, [message]}
      {:ok, _other} -> {:ok, []}
      :error -> :error
    end
  end

  def fast_decode(payload) when is_binary(payload) do
    case parse_payload(payload) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> :error
    end
  end

  defp join_encoded([item]), do: encode_one!(item)
  defp join_encoded([item | rest]), do: [encode_one!(item), ",", join_encoded(rest)]

  defp encode_one!(payload) do
    fields = encode_fields(payload, [])
    ["{", join_fields(Enum.reverse(fields)), "}"]
  end

  defp join_fields([field]), do: field
  defp join_fields([field | rest]), do: [field, ",", join_fields(rest)]
  defp join_fields([]), do: []

  defp encode_fields(payload, acc) do
    acc
    |> maybe_add_int_field(payload, :id)
    |> maybe_add_string_field(payload, :path)
    |> maybe_add_string_field(payload, :stream)
    |> maybe_add_string_field(payload, :sub)
    |> maybe_add_string_field(payload, :unsub)
    |> maybe_add_int_field(payload, :cancel)
    |> maybe_add_string_field(payload, :topic)
    |> maybe_add_json_field(payload, :body)
    |> maybe_add_json_field(payload, :context)
    |> maybe_add_string_field(payload, :error)
    |> maybe_add_bool_field(payload, :done)
  end

  defp maybe_add_int_field(acc, payload, key) do
    case ChannelEx.Message.get(payload, key) do
      value when is_integer(value) -> [[field_name(key), ?:, Integer.to_string(value)] | acc]
      _ -> acc
    end
  end

  defp maybe_add_string_field(acc, payload, key) do
    case ChannelEx.Message.get(payload, key) do
      value when is_binary(value) -> [[field_name(key), ?:, ChannelEx.JSON.encode!(value)] | acc]
      _ -> acc
    end
  end

  defp maybe_add_json_field(acc, payload, key) do
    if ChannelEx.Message.has?(payload, key) do
      value = ChannelEx.Message.get(payload, key)
      [[field_name(key), ?:, ChannelEx.JSON.encode!(value)] | acc]
    else
      acc
    end
  end

  defp maybe_add_bool_field(acc, payload, key) do
    case ChannelEx.Message.get(payload, key) do
      value when is_boolean(value) ->
        [[field_name(key), ?:, if(value, do: "true", else: "false")] | acc]

      _ ->
        acc
    end
  end

  defp field_name(key), do: ChannelEx.JSON.encode!(Atom.to_string(key))

  defp parse_payload(<<"{\"id\":", rest::binary>>) do
    case parse_id_first_message(rest) do
      {:ok, message, <<>>} -> {:ok, [message]}
      _ -> :error
    end
  end

  defp parse_payload(<<"[", rest::binary>>) do
    parse_id_first_array(rest, [])
  end

  defp parse_payload(_payload), do: :error

  defp parse_id_first_array(<<"{\"id\":", rest::binary>>, acc) do
    case parse_id_first_message(rest) do
      {:ok, message, <<",", next::binary>>} ->
        parse_id_first_array(next, [message | acc])

      {:ok, message, <<"]">>} ->
        {:ok, Enum.reverse([message | acc])}

      _ ->
        :error
    end
  end

  defp parse_id_first_array(_payload, _acc), do: :error

  defp parse_id_first_message(payload) do
    with {id, rest} <- parse_int(payload, 0, false),
         {message, rest} <- parse_id_first_field(rest),
         <<"}", rest::binary>> <- rest do
      {:ok, Map.put(message, :id, id), rest}
    else
      _ -> :error
    end
  end

  defp parse_id_first_field(<<",\"path\":\"", rest::binary>>) do
    with {path, rest} <- parse_ascii_string(rest, []),
         {message, rest} <- parse_request_tail(%{path: path}, rest) do
      {message, rest}
    else
      _ -> :error
    end
  end

  defp parse_id_first_field(<<",\"stream\":\"", rest::binary>>) do
    with {path, rest} <- parse_ascii_string(rest, []),
         {message, rest} <- parse_request_tail(%{stream: path}, rest) do
      {message, rest}
    else
      _ -> :error
    end
  end

  defp parse_id_first_field(<<",\"sub\":\"", rest::binary>>) do
    with {path, rest} <- parse_ascii_string(rest, []),
         {message, rest} <- parse_request_tail(%{sub: path}, rest) do
      {message, rest}
    else
      _ -> :error
    end
  end

  defp parse_id_first_field(<<",\"cancel\":", rest::binary>>) do
    case parse_int(rest, 0, false) do
      {cancel, rest} -> {%{cancel: cancel}, rest}
      :error -> :error
    end
  end

  defp parse_id_first_field(<<",\"body\":", rest::binary>>) do
    case parse_json_value(rest) do
      {:ok, body, rest} -> {%{body: body}, rest}
      :error -> :error
    end
  end

  defp parse_id_first_field(<<",\"done\":true", rest::binary>>), do: {%{done: true}, rest}
  defp parse_id_first_field(<<",\"done\":false", rest::binary>>), do: {%{done: false}, rest}

  defp parse_id_first_field(<<",\"error\":\"", rest::binary>>) do
    case parse_ascii_string(rest, []) do
      {error, rest} -> {%{error: error}, rest}
      :error -> :error
    end
  end

  defp parse_id_first_field(<<",\"topic\":\"", rest::binary>>) do
    with {topic, rest} <- parse_ascii_string(rest, []),
         <<",\"body\":", rest::binary>> <- rest,
         {:ok, body, rest} <- parse_json_value(rest) do
      {%{topic: topic, body: body}, rest}
    else
      _ -> :error
    end
  end

  defp parse_id_first_field(_payload), do: :error

  defp parse_request_tail(message, <<",\"body\":", rest::binary>>) do
    with {:ok, body, rest} <- parse_json_value(rest),
         {message, rest} <- parse_request_context(Map.put(message, :body, body), rest) do
      {message, rest}
    else
      _ -> :error
    end
  end

  defp parse_request_tail(message, rest), do: parse_request_context(message, rest)

  defp parse_request_context(message, <<",\"context\":", rest::binary>>) do
    case parse_json_value(rest) do
      {:ok, context, rest} -> {Map.put(message, :context, context), rest}
      :error -> :error
    end
  end

  defp parse_request_context(message, rest), do: {message, rest}

  defp parse_json_value(<<"null", rest::binary>>), do: {:ok, nil, rest}
  defp parse_json_value(<<"true", rest::binary>>), do: {:ok, true, rest}
  defp parse_json_value(<<"false", rest::binary>>), do: {:ok, false, rest}

  defp parse_json_value(<<"\"", rest::binary>>) do
    case parse_ascii_string(rest, []) do
      {value, rest} -> {:ok, value, rest}
      :error -> :error
    end
  end

  defp parse_json_value(<<"{", rest::binary>>), do: parse_json_object(rest, %{})
  defp parse_json_value(<<"[", rest::binary>>), do: parse_json_array(rest, [])

  defp parse_json_value(payload) do
    case parse_int(payload, 0, false) do
      {value, rest} -> {:ok, value, rest}
      :error -> :error
    end
  end

  defp parse_json_object(<<"}", rest::binary>>, acc), do: {:ok, acc, rest}

  defp parse_json_object(<<"\"", rest::binary>>, acc) do
    with {key, rest} <- parse_ascii_string(rest, []),
         <<":", rest::binary>> <- rest,
         {:ok, value, rest} <- parse_json_value(rest) do
      acc = Map.put(acc, key, value)

      case rest do
        <<",", rest::binary>> -> parse_json_object(rest, acc)
        <<"}", rest::binary>> -> {:ok, acc, rest}
        _ -> :error
      end
    else
      _ -> :error
    end
  end

  defp parse_json_object(_payload, _acc), do: :error

  defp parse_json_array(<<"]", rest::binary>>, acc), do: {:ok, Enum.reverse(acc), rest}

  defp parse_json_array(payload, acc) do
    with {:ok, value, rest} <- parse_json_value(payload) do
      case rest do
        <<",", rest::binary>> -> parse_json_array(rest, [value | acc])
        <<"]", rest::binary>> -> {:ok, Enum.reverse([value | acc]), rest}
        _ -> :error
      end
    else
      _ -> :error
    end
  end

  defp parse_int(<<digit, rest::binary>>, acc, _seen?) when digit in ?0..?9 do
    parse_int(rest, acc * 10 + digit - ?0, true)
  end

  defp parse_int(<<"-", rest::binary>>, 0, false) do
    case parse_int(rest, 0, false) do
      {value, rest} -> {-value, rest}
      :error -> :error
    end
  end

  defp parse_int(rest, acc, true), do: {acc, rest}
  defp parse_int(_rest, _acc, false), do: :error

  defp parse_ascii_string(<<"\\\"", rest::binary>>, acc),
    do: parse_ascii_string(rest, [?" | acc])

  defp parse_ascii_string(<<"\\\\", rest::binary>>, acc),
    do: parse_ascii_string(rest, [?\\ | acc])

  defp parse_ascii_string(<<"\\/", rest::binary>>, acc), do: parse_ascii_string(rest, [?/ | acc])
  defp parse_ascii_string(<<"\\b", rest::binary>>, acc), do: parse_ascii_string(rest, [?\b | acc])
  defp parse_ascii_string(<<"\\f", rest::binary>>, acc), do: parse_ascii_string(rest, [?\f | acc])
  defp parse_ascii_string(<<"\\n", rest::binary>>, acc), do: parse_ascii_string(rest, [?\n | acc])
  defp parse_ascii_string(<<"\\r", rest::binary>>, acc), do: parse_ascii_string(rest, [?\r | acc])
  defp parse_ascii_string(<<"\\t", rest::binary>>, acc), do: parse_ascii_string(rest, [?\t | acc])

  defp parse_ascii_string(<<"\"", rest::binary>>, acc) do
    {acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}
  end

  defp parse_ascii_string(<<char, _rest::binary>>, _acc) when char < 0x20, do: :error

  defp parse_ascii_string(<<char, rest::binary>>, acc) when char < 0x80 do
    parse_ascii_string(rest, [char | acc])
  end

  defp parse_ascii_string(_payload, _acc), do: :error
end
