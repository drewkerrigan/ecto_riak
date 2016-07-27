# Ecto Riak

Provides Ecto adapters for Riak (KV), RiakTS, and RiakSearch

## Riak TS

Setup

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

```
defmodule TestRepo do
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
EctoRiak.TestRepo.start_link([])
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
TestRepo.insert!(row)
```

Multiple Rows

```
TestRepo.insert_all(MySchema,
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
TestRepo.get_by(MySchema, region: "myregion", state: "mystate", time: 123456)
```

### Delete Row

```
TestRepo.delete!(row)
```

### Query

```
query = 
    from(e in MySchema, where:
        e.region == "myregion" and
        e.state == "mystate" and
        e.time > 123455 and
        e.time < 123459)
TestRepo.all(query)
```
