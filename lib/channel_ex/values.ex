defmodule ChannelEx.Values do
  @moduledoc false

  alias ChannelEx.Message

  def stream(sender, path, body \\ nil, context \\ nil) when is_binary(path) do
    Stream.resource(
      fn -> start_stream(sender, path, body, context) end,
      &next_value/1,
      &finish_stream/1
    )
  end

  defp start_stream(sender, path, body, context) do
    owner = self()
    ref = make_ref()
    send(sender.owner, {:channel_ex_stream_request, owner, ref, path, body, context})
    %{sender: sender, ref: ref, done: false}
  end

  defp next_value(state) do
    if state.done do
      {:halt, state}
    else
      ref = state.ref

      receive do
        {^ref, response} ->
          case Message.get(response, :error) do
            nil ->
              if Message.get(response, :done, false) do
                {:halt, %{state | done: true}}
              else
                {[Message.get(response, :body)], state}
              end

            error ->
              raise RuntimeError, message: to_string(error)
          end
      end
    end
  end

  defp finish_stream(%{done: true}), do: :ok

  defp finish_stream(state) do
    send(state.sender.owner, {:channel_ex_cancel_request, state.ref})
    :ok
  end
end
