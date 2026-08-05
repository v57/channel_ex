defmodule ChannelEx.Core do
  @moduledoc false

  def register_publishers(%{events_api: nil}, _publish_fun), do: []

  def register_publishers(channel, publish_fun) when is_function(publish_fun, 1) do
    Enum.map(channel.events_api, fn {_name, subscription} ->
      id = ChannelEx.Subscription.add_publisher(subscription, publish_fun)
      {subscription, id}
    end)
  end

  def cleanup_publishers(refs) do
    Enum.each(refs, fn {subscription, id} ->
      ChannelEx.Subscription.remove_publisher(subscription, id)
    end)

    :ok
  end

  def invoke_post(channel, sender, state_ref, path, body, context) do
    descriptor =
      Map.get(channel.post_api, path) ||
        Enum.find_value(channel.other_post_api, fn {match, maybe_descriptor} ->
          if match.(path), do: maybe_descriptor
        end)
        |> resolve_post_descriptor()

    if is_nil(descriptor) do
      {:error, "api not found"}
    else
      request = %{
        body: body,
        sender: sender,
        state: state_ref,
        path: path,
        context: normalize_context(context),
        task: nil,
        cancelled?: fn -> false end,
        is_cancelled?: fn -> false end
      }

      case run_safe(fn -> descriptor.fun.(request) end) do
        {:ok, value} ->
          if value == ChannelEx.no_reply() do
            {:ok, nil}
          else
            {:ok, value}
          end

        other ->
          other
      end
    end
  end

  def invoke_stream(channel, sender, state_ref, path, body, context) do
    stream_api =
      Map.get(channel.stream_api, path) ||
        Enum.find_value(channel.other_stream_api, fn {match, request} ->
          if match.(path), do: request
        end)

    cond do
      is_nil(stream_api) ->
        {:error, "api not found"}

      true ->
        values = []

        emitter = %{
          emit: fn item ->
            Process.put({__MODULE__, :emitted_values}, [
              item | Process.get({__MODULE__, :emitted_values}, [])
            ])

            :ok
          end
        }

        request = %{
          body: body,
          sender: sender,
          state: state_ref,
          path: path,
          context: normalize_context(context),
          emitter: emitter,
          cancelled?: fn -> false end,
          is_cancelled?: fn -> false end
        }

        Process.put({__MODULE__, :emitted_values}, [])

        result =
          case run_safe(fn -> stream_api.(request) end) do
            {:ok, nil} ->
              {:ok, []}

            {:ok, returned} ->
              emitted = Process.get({__MODULE__, :emitted_values}, [])

              cond do
                emitted != [] ->
                  {:ok, Enum.reverse(emitted)}

                Enumerable.impl_for(returned) != nil ->
                  {:ok, Enum.to_list(returned)}

                true ->
                  {:error, "stream must return iterable"}
              end

            {:error, _error} = error ->
              error
          end

        Process.delete({__MODULE__, :emitted_values})
        _ = values
        result
    end
  end

  def invoke_subscribe(channel, sub, request_body) do
    subscription = channel.events_api && Map.get(channel.events_api, sub)

    if is_nil(subscription) do
      {:error, "subscription not found"}
    else
      case run_safe(fn ->
             topic = ChannelEx.Subscription.get_topic(subscription, request_body)
             prefix = ChannelEx.Subscription.prefix(subscription)

             full_topic =
               if to_string(topic) == "" do
                 prefix
               else
                 prefix <> "/" <> to_string(topic)
               end

             body = ChannelEx.Subscription.get_body(subscription, request_body)
             {full_topic, body}
           end) do
        {:ok, {topic, body}} -> {:ok, topic, body}
        {:error, _error} = error -> error
      end
    end
  end

  def dispatch_local_event(callbacks, topic, body) do
    callbacks
    |> Map.get(topic, %{})
    |> Enum.each(fn {_id, callback} ->
      run_safe(fn -> callback.(body) end)
    end)

    :ok
  end

  def remote_topics_add(remote_topics, topic), do: MapSet.put(remote_topics, topic)
  def remote_topics_remove(remote_topics, topic), do: MapSet.delete(remote_topics, topic)

  def normalize_context(nil), do: %{}
  def normalize_context(context), do: context

  def run_disconnect(channel, state_ref, sender) do
    case channel.on_disconnect do
      fun when is_function(fun, 2) ->
        _ = run_safe(fn -> fun.(state_ref, sender) end)
        :ok

      _ ->
        :ok
    end
  end

  def maybe_send_topic(publish_fun, remote_topics, topic, body)
      when is_function(publish_fun, 2) and is_binary(topic) do
    if MapSet.member?(remote_topics, topic) and not is_nil(body) do
      publish_fun.(topic, body)
    else
      :ok
    end
  end

  defp run_safe(fun) when is_function(fun, 0) do
    try do
      {:ok, fun.()}
    rescue
      error -> {:error, Exception.message(error)}
    catch
      kind, reason -> {:error, "#{kind}: #{inspect(reason)}"}
    end
  end

  defp resolve_post_descriptor(%{mode: mode, fun: fun} = descriptor)
       when mode in [:sync, :async] and is_function(fun, 1),
       do: descriptor

  defp resolve_post_descriptor(fun) when is_function(fun, 1), do: %{mode: :sync, fun: fun}
  defp resolve_post_descriptor(_), do: nil
end
