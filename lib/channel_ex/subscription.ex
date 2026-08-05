defmodule ChannelEx.Subscription do
  @moduledoc false

  defstruct [:publishers_table, :meta_table, :next_id, :topic_fun, :body_fun]

  def new do
    publishers_table =
      :ets.new(:channel_ex_subscription_publishers, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    meta_table =
      :ets.new(:channel_ex_subscription_meta, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    true = :ets.insert(meta_table, {:prefix, ""})
    next_id = :atomics.new(1, [])

    %__MODULE__{
      publishers_table: publishers_table,
      meta_table: meta_table,
      next_id: next_id,
      topic_fun: fn
        nil -> ""
        value -> to_string(value)
      end,
      body_fun: fn _ -> nil end
    }
  end

  def topic(%__MODULE__{} = subscription, fun) when is_function(fun, 1) do
    %{subscription | topic_fun: fun}
  end

  def body(%__MODULE__{} = subscription, fun) when is_function(fun, 1) do
    %{subscription | body_fun: fun}
  end

  def set_body(%__MODULE__{} = subscription, fun) when is_function(fun, 1) do
    %{subscription | body_fun: fun}
  end

  def set_prefix(%__MODULE__{} = subscription, prefix) when is_binary(prefix) do
    true = :ets.insert(subscription.meta_table, {:prefix, prefix})
    subscription
  end

  def add_publisher(%__MODULE__{} = subscription, publish_fun) when is_function(publish_fun, 1) do
    id = :atomics.add_get(subscription.next_id, 1, 1)
    true = :ets.insert(subscription.publishers_table, {id, publish_fun})
    id
  end

  def remove_publisher(%__MODULE__{} = subscription, id) do
    if :ets.info(subscription.publishers_table) != :undefined do
      :ets.delete(subscription.publishers_table, id)
    end

    :ok
  end

  def publisher_count(%__MODULE__{} = subscription) do
    case :ets.info(subscription.publishers_table, :size) do
      :undefined -> 0
      size -> size
    end
  end

  def send(%__MODULE__{} = subscription, request, body \\ :__channel_ex_missing__) do
    topic = safe_call_topic(subscription.topic_fun, request)

    computed_body =
      if body == :__channel_ex_missing__ do
        safe_call_body(subscription.body_fun, request)
      else
        body
      end

    publish(subscription, topic, computed_body)
  end

  def publish(%__MODULE__{} = subscription, topic, body) do
    if is_nil(body) do
      :ok
    else
      prefix = prefix(subscription)

      final_topic =
        if prefix == "" do
          to_string(topic)
        else
          prefix <> "/" <> to_string(topic)
        end

      event = %{topic: final_topic, body: body}

      subscription.publishers_table
      |> :ets.tab2list()
      |> Enum.each(fn {_id, publish_fun} ->
        safe_publish(publish_fun, event)
      end)
    end
  end

  def get_topic(%__MODULE__{} = subscription, request) do
    safe_call_topic(subscription.topic_fun, request)
  end

  def get_body(%__MODULE__{} = subscription, request) do
    safe_call_body(subscription.body_fun, request)
  end

  def prefix(%__MODULE__{} = subscription) do
    case :ets.lookup(subscription.meta_table, :prefix) do
      [{:prefix, prefix}] -> prefix
      _ -> ""
    end
  end

  defp safe_call_topic(fun, request) do
    fun.(request)
  rescue
    error -> reraise error, __STACKTRACE__
  catch
    kind, reason -> :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp safe_call_body(fun, request) do
    fun.(request)
  rescue
    error -> reraise error, __STACKTRACE__
  catch
    kind, reason -> :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp safe_publish(fun, event) do
    fun.(event)
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end
