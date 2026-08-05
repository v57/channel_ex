defmodule ChannelEx.Message do
  @moduledoc false

  def get(map, key, default \\ nil)

  def get(map, key, default) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, atom_value} ->
        atom_value

      :error ->
        case map_key(key) do
          nil -> Map.get(map, Atom.to_string(key), default)
          string_key -> Map.get(map, string_key, default)
        end
    end
  end

  def get(map, key, default) when is_map(map), do: Map.get(map, key, default)

  def has?(map, key) when is_map(map) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  def with_id(payload, id) when is_map(payload) do
    payload
    |> Map.put(:id, id)
    |> Map.delete("id")
  end

  defp map_key(:path), do: "path"
  defp map_key(:stream), do: "stream"
  defp map_key(:cancel), do: "cancel"
  defp map_key(:sub), do: "sub"
  defp map_key(:unsub), do: "unsub"
  defp map_key(:topic), do: "topic"
  defp map_key(:id), do: "id"
  defp map_key(:body), do: "body"
  defp map_key(:context), do: "context"
  defp map_key(:done), do: "done"
  defp map_key(:error), do: "error"
  defp map_key(_key), do: nil
end
