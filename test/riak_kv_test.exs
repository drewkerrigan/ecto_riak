defmodule EctoRiakKVTest do
  use EctoRiak.Case
  require EctoRiak.RiakKVRepo, as: RiakKVRepo

  alias Ecto.Riak.Register
  alias Ecto.Riak.Counter
  alias Ecto.Riak.Flag
  alias Ecto.Riak.Map
  alias Ecto.Riak.Set

  defmodule MyMapSchema do
    use Ecto.Schema

    schema "maps/testbucket" do
      field :myregister, Register
      field :mycounter, Counter
      field :myflag, Flag
      field :mymap, Map
      field :myset, Set
    end
  end

  test "insert, instert all, get_by, delete" do
    obj = %MyMapSchema{
      myregister: %Register{} |> Register.set("hello"),
      mycounter: %Counter{} |> Counter.increment(1),
      myflag: %Flag{} |> Flag.enable,
      mymap: %Map{} |> Map.put("test", %Register{} |> Register.set("there")),
      myset: %Set{} |> Set.put("one")
    }
    inserted_obj = RiakKVRepo.insert!(obj)
    assert obj.mycounter |> Counter.value == inserted_obj.mycounter |> Counter.value
    # assert {2, nil} == RiakKVRepo.insert_all(MySchema,
    #   [%{region: "myregion2",
    #      state: "mystate2",
    #      time: 123457,
    #      weather: "windy",
    #      temperature: 55.0},
    #    %{region: "myregion3",
    #      state: "mystate3",
    #      time: 123458,
    #      weather: "rainy",
    #      temperature: 45.0}])
    # returned_row = RiakKVRepo.get_by(MySchema, region: "myregion", state: "mystate", time: 123456)
    # assert row.region == returned_row.region
    # deleted_row = RiakKVRepo.delete!(row)
    # assert :deleted == deleted_row.__meta__.state
  end

  # test "queries" do
  #   %MySchema{
  #     region: "myregion",
  #     state: "mystate",
  #     time: 123456,
  #     weather: "sunny",
  #     temperature: 65.0
  #   }
  #   |> RiakKVRepo.insert!
  #   query = from(e in MySchema, where:
  #     e.region == "myregion" and
  #     e.state == "mystate" and
  #     e.time > 123455 and
  #     e.time < 123459)

  #   [result] = RiakKVRepo.all(query)
  #   assert "myregion" == result.region
  # end
end
