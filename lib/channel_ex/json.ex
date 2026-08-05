defmodule ChannelEx.JSON do
  @moduledoc false

  if Code.ensure_loaded?(:jiffy) do
    def encode!(value) do
      try do
        :jiffy.encode(value)
      rescue
        _ -> Jason.encode!(value)
      catch
        _, _ -> Jason.encode!(value)
      end
    end

    def decode!(binary) when is_binary(binary) do
      :jiffy.decode(binary, [:return_maps])
    end
  else
    def encode!(value) do
      Jason.encode!(value)
    end

    def decode!(binary) when is_binary(binary) do
      Jason.decode!(binary)
    end
  end

  def decode_any(binary) when is_binary(binary) do
    case decode!(binary) do
      value when is_list(value) -> value
      value -> [value]
    end
  end
end
