use Mix.Config

config :ecto, EctoRiak.RiakTSRepo,
  adapter: Ecto.Adapters.RiakTS,
  pool: [
    [host: '127.0.0.1', port: 8087]
  ]

config :ecto, EctoRiak.RiakKVRepo,
  adapter: Ecto.Adapters.RiakKV,
  host: "localhost",
  port: 8087

config :ecto, EctoRiak.RiakDTRepo,
  adapter: Ecto.Adapters.RiakDT,
  host: "localhost",
  port: 8087
