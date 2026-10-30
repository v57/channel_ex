defmodule ChannelEx.JSON do
  @moduledoc false

  if Code.ensure_loaded?(:jiffy) do
    # :use_nil makes jiffy write nil as null and read null as nil, like Jason; without it nil
    # goes out as the string "nil" and null comes back as :null.
    def encode!(value) do
      try do
        :jiffy.encode(value, [:use_nil])
      rescue
        _ -> Jason.encode!(value)
      catch
        _, _ -> Jason.encode!(value)
      end
    end

    def decode!(binary) when is_binary(binary) do
      :jiffy.decode(binary, [:return_maps, :use_nil])
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
