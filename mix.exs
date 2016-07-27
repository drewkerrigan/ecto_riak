defmodule EctoRiak.Mixfile do
  use Mix.Project

  def project do
    [app: :ecto_riak,
     version: "0.0.1",
     elixir: "~> 1.3",
     deps: deps]
  end

  def application do
    [applications: [:riak, :logger]]
  end

  defp deps do
    [
      {:ecto,    "~> 2.0"},
      {:decimal, "~> 1.1"},
      {:poison,  "~> 2.2"},
      {:riak,   "~> 1.1"}
    ]
  end
end
