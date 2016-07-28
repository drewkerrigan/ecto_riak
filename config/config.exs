use Mix.Config

## Ecto.Adapters.Riak* configuration options:
#
# config :ecto, EctoRiak.RiakTSRepo,
#   adapter: Ecto.Adapters.RiakTS,
#   hostname: "localhost",
#   port: 8087,
#   init_count: 5,
#   max_count: 10
#
# config :ecto, EctoRiak.RiakTSRepo,
#   adapter: Ecto.Adapters.RiakTS,
#   pool: [
#     [name: :riak1,
#      group: :riak,
#      max_count: 10,
#      init_count: 5,
#      host: '127.0.0.1',
#      port: 8087]
#   ]
#
# config :ecto, EctoRiak.RiakTSRepo,
#   adapter: Ecto.Adapters.RiakTS,
#   hostname: "localhost",
#   port: 8087,
#   init_count: 5,
#   max_count: 10,
#   pool: [
#     [name: :riak1,
#      group: :riak,
#      max_count: 10,
#      init_count: 5,
#      start_mfa: { Riak.Connection, :start_link, ['127.0.0.1', 8087] }]
#   ]
#
# config :ecto, EctoRiak.RiakTSRepo,
#   adapter: Ecto.Adapters.RiakTS,
#   pool: [
#     [host: 'riak1.host.com', port: 8087],
#     [host: 'riak2.host.com', port: 8087],
#     [host: 'riak3.host.com', port: 8087],
#     [host: 'riak4.host.com', port: 8087],
#     [host: 'riak5.host.com', port: 8087],
#   ]

import_config "#{Mix.env}.exs"
