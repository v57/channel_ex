defmodule ChannelEx.QuicFrameTest do
  use ExUnit.Case, async: true

  alias ChannelEx.Quic.Frame

  test "lengths use the shortest varint, and a keepalive in front is skipped" do
    for size <- [0, 1, 63, 64, 16383, 16384, 70_000] do
      payload = :binary.copy("x", size)
      frame = payload |> Frame.encode() |> IO.iodata_to_binary()

      header =
        cond do
          size < 64 -> 1
          size < 16384 -> 2
          true -> 4
        end

      assert byte_size(frame) - size == header

      buffer = IO.iodata_to_binary([Frame.keepalive(), frame, Frame.encode("next")])
      expected = if size > 0, do: [payload, "next"], else: ["next"]
      assert {:ok, ^expected, <<>>} = Frame.decode(buffer)
    end
  end

  test "8-byte lengths decode" do
    assert {:ok, ["hello"], <<>>} = Frame.decode(<<0xC0, 0, 0, 0, 0, 0, 0, 5, "hello">>)
    assert IO.iodata_to_binary(Frame.varint(Bitwise.bsl(1, 40))) == <<0xC0, 0, 1, 0, 0, 0, 0, 0>>
  end

  test "an incomplete frame is kept for the next read, byte by byte" do
    frame = IO.iodata_to_binary(Frame.encode(:binary.copy("é", 100)))

    {payloads, rest} =
      for <<byte <- frame>>, reduce: {[], <<>>} do
        {payloads, rest} ->
          {:ok, new, rest} = Frame.decode(rest <> <<byte>>)
          {payloads ++ new, rest}
      end

    assert payloads == [:binary.copy("é", 100)]
    assert rest == <<>>
  end

  test "a frame over the limit is refused from its header alone" do
    frame = IO.iodata_to_binary(Frame.encode(:binary.copy("x", 11)))
    assert {:error, :too_large} = Frame.decode(frame, 10)
    assert {:ok, [_], <<>>} = Frame.decode(frame, 11)
    # 16 MiB + 1 announced, nothing else sent yet
    assert {:error, :too_large} = Frame.decode(<<0x81, 0, 0, 1>>)
    assert Frame.default_max_payload() == 16 * 1024 * 1024
  end
end
