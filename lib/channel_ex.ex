defmodule ChannelEx do
  @moduledoc false

  @no_reply_marker {:__channel_ex__, :no_reply}

  defstruct post_api: %{},
            other_post_api: [],
            stream_api: %{},
            other_stream_api: [],
            events_api: nil,
            on_disconnect: nil

  def new do
    %__MODULE__{}
  end

  def no_reply, do: @no_reply_marker

  def post(%__MODULE__{} = channel, path, request)
      when is_binary(path) and is_function(request, 1) do
    %{channel | post_api: Map.put(channel.post_api, path, %{mode: :sync, fun: request})}
  end

  def post_async(%__MODULE__{} = channel, path, request)
      when is_binary(path) and is_function(request, 1) do
    post(channel, path, request)
  end

  def post_other(%__MODULE__{} = channel, path_fun, request)
      when is_function(path_fun, 1) and is_function(request, 1) do
    %{
      channel
      | other_post_api: channel.other_post_api ++ [{path_fun, %{mode: :sync, fun: request}}]
    }
  end

  def post_other_async(%__MODULE__{} = channel, path_fun, request)
      when is_function(path_fun, 1) and is_function(request, 1) do
    post_other(channel, path_fun, request)
  end

  def stream(%__MODULE__{} = channel, path, request)
      when is_binary(path) and is_function(request, 1) do
    %{channel | stream_api: Map.put(channel.stream_api, path, request)}
  end

  def stream_other(%__MODULE__{} = channel, path_fun, request)
      when is_function(path_fun, 1) and is_function(request, 1) do
    %{channel | other_stream_api: channel.other_stream_api ++ [{path_fun, request}]}
  end

  def on_disconnect(%__MODULE__{} = channel, action) when is_function(action, 2) do
    %{channel | on_disconnect: action}
  end

  def events(channel, events, prefix \\ "")

  def events(%__MODULE__{} = channel, events, prefix) when is_map(events) and is_binary(prefix) do
    events_api = channel.events_api || %{}
    %{channel | events_api: parse_subscriptions(events, prefix, events_api)}
  end

  def api(%__MODULE__{} = channel, api_map) when is_map(api_map) do
    parse_api(channel, api_map, "")
  end

  def merge(%__MODULE__{} = channel, %__MODULE__{} = from, prefix \\ "") do
    p = prefixed(prefix)

    merged =
      Enum.reduce(from.post_api, channel, fn {key, descriptor}, acc ->
        post(acc, p <> key, descriptor.fun)
      end)

    merged =
      Enum.reduce(from.other_post_api, merged, fn {path_fun, descriptor}, acc ->
        post_other(acc, path_fun, descriptor.fun)
      end)

    merged =
      Enum.reduce(from.stream_api, merged, fn {key, fun}, acc ->
        stream(acc, p <> key, fun)
      end)

    if from.events_api do
      Enum.reduce(from.events_api, merged, fn {key, subscription}, acc ->
        events(acc, %{key => subscription}, prefix)
      end)
    else
      merged
    end
  end

  def connect(%__MODULE__{} = channel, address, options \\ []) do
    ChannelEx.Client.connect(channel, address, options)
  end

  def send(target, path, body \\ nil, context \\ nil, timeout \\ 5_000)

  def send(%ChannelEx.ClientSender{} = client, path, body, context, timeout) do
    ChannelEx.ClientSender.send(client, path, body, context, timeout)
  end

  def send(%ChannelEx.Sender{} = sender, path, body, context, timeout) do
    ChannelEx.Sender.send(sender, path, body, context, timeout)
  end

  # A failed `connect/3` returns `{:error, reason}`; sending on it reports that reason.
  def send({:error, _reason} = error, _path, _body, _context, _timeout), do: error

  def values(target, path, body \\ nil, context \\ nil)

  def values(%ChannelEx.ClientSender{} = client, path, body, context) do
    ChannelEx.ClientSender.values(client, path, body, context)
  end

  def values(%ChannelEx.Sender{} = sender, path, body, context) do
    ChannelEx.Sender.values(sender, path, body, context)
  end

  def listen(%__MODULE__{} = channel, address, options \\ []) do
    ChannelEx.Server.listen(channel, address, options)
  end

  def process(%__MODULE__{} = channel, options \\ []) do
    ChannelEx.Process.process(channel, options)
  end

  def make_sender(%__MODULE__{} = channel, owner) when is_pid(owner) do
    ChannelEx.Sender.new(channel, owner, :compat, nil)
  end

  def cancel(_channel, _id), do: :ok
  def disconnect(_channel, _state, _sender), do: :ok

  defp parse_api(channel, api_map, prefix) do
    Enum.reduce(api_map, channel, fn {key, value}, acc ->
      key = to_string(key)
      path = if key == "_", do: prefix, else: prefixed_path(prefix, key)

      cond do
        is_function(value, 1) ->
          post(acc, path, value)

        match?({:async, _fun}, value) ->
          {:async, post_fun} = value
          post_async(acc, path, post_fun)

        match?({:stream, _fun}, value) ->
          {:stream, stream_fun} = value
          stream(acc, path, stream_fun)

        is_map(value) ->
          parse_api(acc, value, path)

        true ->
          acc
      end
    end)
  end

  defp parse_subscriptions(events, prefix, map) do
    p = prefixed(prefix)

    Enum.reduce(events, map, fn {key, value}, acc ->
      key = to_string(key)

      cond do
        match?(%ChannelEx.Subscription{}, value) ->
          topic_prefix = if prefix == "", do: key, else: prefix <> "/" <> key
          subscription = ChannelEx.Subscription.set_prefix(value, topic_prefix)
          Map.put(acc, p <> key, subscription)

        is_map(value) ->
          parse_subscriptions(value, prefix <> key <> "/", acc)

        true ->
          acc
      end
    end)
  end

  defp prefixed(""), do: ""
  defp prefixed(prefix), do: prefix <> "/"

  defp prefixed_path("", key), do: key
  defp prefixed_path(prefix, key), do: prefix <> "/" <> key
end
