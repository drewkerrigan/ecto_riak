# Ecto Riak

Provides Ecto adapters for Riak (KV), RiakTS, and RiakSearch

## Riak TS

Setup

Create a table

```
riak-admin bucket-type create GeoCheckin '{"props":{"table_def": 
    "CREATE TABLE GeoCheckin (
        region VARCHAR NOT NULL, 
        state VARCHAR NOT NULL, 
        time TIMESTAMP NOT NULL, 
        weather VARCHAR NOT NULL, 
        temperature DOUBLE, 
        PRIMARY KEY ((region, state, QUANTUM(time, 15, 'm')), 
        region, state, time))"}}'
riak-admin bucket-type activate GeoCheckin
```

Configure

Examples of adapter configuration options can be found [here](config/config.exs). Create a `config/config.exs`:

```
config :ecto, EctoRiak.RiakTSRepo,
  adapter: Ecto.Adapters.RiakTS,
  hostname: "localhost",
  port: 8087,
  init_count: 5,
  max_count: 10
```

Create a repo

```
defmodule RiakTSRepo do
  use Ecto.Repo, otp_app: :ecto, adapter: Ecto.Adapters.RiakTS
end

defmodule MySchema do
  use Ecto.Schema

  @primary_key false

  schema "GeoCheckin" do
    field :region, :string, primary_key: true
    field :state, :string, primary_key: true
    field :time, :integer, primary_key: true
    field :weather, :string
    field :temperature, :float
  end
end
```

Start

```
EctoRiak.RiakTSRepo.start_link()
```

or to override configured host / port:

```
{:ok, pid } = EctoRiak.RiakTSRepo.start_link(url: "ecto://locahost:8087/default")
```


### Insert Rows

Single Row

```
row = %MySchema{
  region: "myregion",
  state: "mystate",
  time: 123456,
  weather: "sunny",
  temperature: 65.0
}
RiakTSRepo.insert!(row)
```

Multiple Rows

```
RiakTSRepo.insert_all(MySchema,
  [%{region: "myregion2",
     state: "mystate2",
     time: 123457,
     weather: "windy",
     temperature: 55.0},
   %{region: "myregion3",
     state: "mystate3",
     time: 123458,
     weather: "rainy",
     temperature: 45.0}])
```

### Get Row

```
RiakTSRepo.get_by(MySchema, region: "myregion", state: "mystate", time: 123456)
```

### Delete Row

```
RiakTSRepo.delete!(row)
```

### Query

```
query = 
    from(e in MySchema, where:
        e.region == "myregion" and
        e.state == "mystate" and
        e.time > 123455 and
        e.time < 123459)
RiakTSRepo.all(query)
```
