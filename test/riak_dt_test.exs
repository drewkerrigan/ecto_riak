defmodule EctoRiakDTTest do
  use EctoRiak.Case
  require EctoRiak.RiakDTRepo, as: RiakDTRepo
  require Logger

  alias Ecto.Riak.Counter
  # alias Ecto.Riak.Flag
  # alias Ecto.Riak.Set
  # alias Ecto.Riak.Register

  # defmodule MyMapSchema do
  #   use Ecto.Schema

  #   schema "maps/testbucket" do
  #     field :mycounter, Counter
  #   end
  # end

  defmodule Permalink do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    embedded_schema do
      field :url, :string
    end
  end

  defmodule Post do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "maps.posts" do
      field :title, :string
      field :body, :string
      field :views, Counter
      field :active, :boolean
      field :tags, {:array, :string}
      embeds_one :permalink, Permalink
    end
  end

  test "insert, instert all, get_by, delete" do

    link = %Permalink{url: "http://mysite.com/link"}

    obj = %Post{
      title: "My Post",
      body: "<html><body>The contents.</body></html>",
      views: Counter.new |> Counter.increment(1),
      active: false,
      tags: ["some_tag"],
      permalink: link
    }

    inserted_obj = RiakDTRepo.insert!(obj)
    assert "http://mysite.com/link" == inserted_obj.permalink.url

    returned_row = RiakDTRepo.get(Post, inserted_obj.id)

    Logger.info("returned: #{inspect returned_row}")
    assert obj.active == returned_row.active

    updated_row = %{returned_row | views: returned_row.views |> Counter.increment(1)}
    Logger.info("updated: #{inspect updated_row}")

    RiakDTRepo.insert!(updated_row)

    # assert {2, nil} == RiakDTRepo.insert_all(MySchema,
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
    # returned_row = RiakDTRepo.get_by(MySchema, region: "myregion", state: "mystate", time: 123456)
    # assert row.region == returned_row.region
    # deleted_row = RiakDTRepo.delete!(row)
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
  #   |> RiakDTRepo.insert!
  #   query = from(e in MySchema, where:
  #     e.region == "myregion" and
  #     e.state == "mystate" and
  #     e.time > 123455 and
  #     e.time < 123459)

  #   [result] = RiakDTRepo.all(query)
  #   assert "myregion" == result.region
  # end
end
