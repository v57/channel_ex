defmodule ChannelEx.ConnectionState do
  @moduledoc false

  defstruct [:table]

  def start_link(initial, opts \\ []) do
    _ = opts

    table =
      :ets.new(:channel_ex_connection_state, [
        :set,
        :public,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])

    true = :ets.insert(table, {:value, initial})
    {:ok, %__MODULE__{table: table}}
  end

  def get(%__MODULE__{table: table}) do
    case :ets.lookup(table, :value) do
      [{:value, value}] -> value
      _ -> nil
    end
  end

  def put(%__MODULE__{table: table}, state) do
    true = :ets.insert(table, {:value, state})
    :ok
  end

  def update(%__MODULE__{} = ref, fun) when is_function(fun, 1) do
    ref
    |> get()
    |> fun.()
    |> then(&put(ref, &1))
  end

  def stop(%__MODULE__{table: table}) do
    if :ets.info(table) != :undefined do
      :ets.delete(table)
    end

    :ok
  end
end
