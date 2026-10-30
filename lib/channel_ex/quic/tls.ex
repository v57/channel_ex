if Code.ensure_loaded?(:quicer) do
  defmodule ChannelEx.Quic.TLS do
    @moduledoc false
    # quicer (msquic) reads certificates, keys and CAs from files only, so PEM given in memory is
    # written to a private temporary file for the call that loads it, then removed.

    @alpn ~c"channel/1"

    def alpn, do: @alpn

    @doc "Runs `fun` with each PEM in `pems` (nil stays nil) written to its own temporary file."
    def with_pem_files(pems, fun) do
      paths = Enum.map(pems, &write_pem/1)

      try do
        fun.(paths)
      after
        Enum.each(paths, fn
          nil -> :ok
          path -> File.rm(path)
        end)
      end
    end

    defp write_pem(nil), do: nil

    defp write_pem(pem) do
      dir = Path.join(System.tmp_dir!(), "channel_ex_quic")
      File.mkdir_p!(dir)
      File.chmod!(dir, 0o700)
      path = Path.join(dir, Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false))
      File.write!(path, IO.iodata_to_binary(pem))
      File.chmod!(path, 0o600)
      String.to_charlist(path)
    end

    @doc "A SHA-256 fingerprint in hex, colons and case ignored, as 32 bytes; nil or :error."
    def parse_fingerprint(nil), do: nil

    def parse_fingerprint(fingerprint) when is_binary(fingerprint) do
      case fingerprint |> String.replace(":", "") |> Base.decode16(case: :mixed) do
        {:ok, <<_::binary-size(32)>> = digest} -> digest
        _ -> :error
      end
    end

    def parse_fingerprint(_other), do: :error
  end
end
