defmodule ChannelEx.Quic.Frame do
  @moduledoc false
  # QUIC transport framing (channel/docs/quic.md): each message is its length as an RFC 9000
  # variable-length integer, then that many bytes of UTF-8 JSON. An empty frame is a keepalive.
  # Pure code with no quicer dependency, so it's always compiled.

  @default_max_payload 16 * 1024 * 1024

  def default_max_payload, do: @default_max_payload

  @doc "The keepalive frame: a zero length."
  def keepalive, do: <<0>>

  @doc "Length prefix and payload of one message, as iodata."
  def encode(payload) do
    [varint(IO.iodata_length(payload)), payload]
  end

  @doc "RFC 9000 section 16 variable-length integer, in the shortest form."
  def varint(n) when n < 0x40, do: <<n>>
  def varint(n) when n < 0x4000, do: <<0b01::2, n::14>>
  def varint(n) when n < 0x40000000, do: <<0b10::2, n::30>>
  def varint(n), do: <<0b11::2, n::62>>

  @doc """
  Splits `buffer` into complete frame payloads, skipping keepalives.

  Returns `{:ok, payloads, rest}`, where `rest` is an incomplete frame to keep for the next
  read, or `{:error, :too_large}` once a frame's length exceeds `max_payload`.
  """
  def decode(buffer, max_payload \\ @default_max_payload), do: decode(buffer, max_payload, [])

  defp decode(<<>>, _max, acc), do: {:ok, Enum.reverse(acc), <<>>}

  defp decode(<<prefix::2, _::bitstring>> = buffer, max, acc) do
    size = Bitwise.bsl(1, prefix)
    bits = size * 8 - 2

    case buffer do
      <<_::2, length::size(^bits), _rest::binary>> when length > max ->
        {:error, :too_large}

      <<_::2, 0::size(^bits), rest::binary>> ->
        decode(rest, max, acc)

      <<_::2, length::size(^bits), payload::binary-size(length), rest::binary>> ->
        decode(rest, max, [payload | acc])

      _incomplete ->
        {:ok, Enum.reverse(acc), buffer}
    end
  end
end
