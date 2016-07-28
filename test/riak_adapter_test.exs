defmodule EctoRiakTest do
  use EctoRiak.Case

  import Ecto.Query
  require EctoRiak.TestRepo, as: TestRepo

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

  test "insert, instert all, get_by, delete" do
    row = %MySchema{
      region: "myregion",
      state: "mystate",
      time: 123456,
      weather: "sunny",
      temperature: 65.0
    }
    inserted_row = TestRepo.insert!(row)
    assert row.region == inserted_row.region
    assert {2, nil} == TestRepo.insert_all(MySchema,
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
    returned_row = TestRepo.get_by(MySchema, region: "myregion", state: "mystate", time: 123456)
    assert row.region == returned_row.region
    deleted_row = TestRepo.delete!(row)
    assert :deleted == deleted_row.__meta__.state
  end

  test "queries" do
    %MySchema{
      region: "myregion",
      state: "mystate",
      time: 123456,
      weather: "sunny",
      temperature: 65.0
    }
    |> TestRepo.insert!
    query = from(e in MySchema, where:
      e.region == "myregion" and
      e.state == "mystate" and
      e.time > 123455 and
      e.time < 123459)

    [result] = TestRepo.all(query)
    assert "myregion" == result.region
  end
end
