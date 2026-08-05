defmodule ChannelEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :channel_ex,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      test_coverage: [tool: ExCoveralls],
      deps: deps()
    ]
  end

  def cli do
    [preferred_envs: preferred_cli_env()]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssl, :tools],
      mod: {ChannelEx.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jiffy, "~> 1.1"},
      {:jason, "~> 1.4"},
      {:excoveralls, "~> 0.18", only: :test}
    ]
  end

  defp preferred_cli_env do
    [
      coveralls: :test,
      "coveralls.html": :test,
      "coveralls.detail": :test,
      "coveralls.post": :test,
      "coveralls.github": :test,
      "coveralls.lcov": :test
    ]
  end
end
