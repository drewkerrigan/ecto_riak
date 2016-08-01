defmodule EctoRiakDTTest do
  use EctoRiak.Case
  require EctoRiak.RiakDTRepo, as: RiakDTRepo
  require Logger
  import Ecto.Query

  alias Ecto.Riak.Counter
  alias Ecto.Riak.Set

  defmodule Permalink do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    embedded_schema do
      field :url, :string
    end
  end

  defmodule Post do
    use Ecto.Riak.Schema

    @primary_key {:id, :binary_id, autogenerate: true}

    schema "maps.posts" do
      field :title, :string
      field :body, :string
      field :views, Counter #TODO also implement :integer impl
      field :active, :boolean
      field :tags, Set
      term_index :tags, "sets.posts_index"
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
      tags: Set.new |> Set.put("some_tag"),
      permalink: link
    }


    inserted_obj = RiakDTRepo.insert!(obj)
    assert "http://mysite.com/link" == inserted_obj.permalink.url


    returned_row = RiakDTRepo.get(Post, inserted_obj.id)
    assert obj.active == returned_row.active

    updated_row = %{returned_row |
                    views: returned_row.views |> Counter.increment(1),
                    active: true,
                    tags: returned_row.tags |> Set.put("another_tag"),
                    permalink: %{returned_row.permalink |
                                 id: returned_row.permalink.id,
                                 url: "http://anothersite.com/link"}}

    RiakDTRepo.insert!(updated_row)
    returned_row = RiakDTRepo.get(Post, returned_row.id)
    assert true == returned_row.active

    assert {2, nil} == RiakDTRepo.insert_all(Post,
      [%{
          title: "My Post2",
          body: "<html><body>The contents.</body></html>",
          views: Counter.new |> Counter.increment(1),
          active: false,
          tags: Set.new |> Set.put("some_tag"),
          permalink: link
       },
       %{
         title: "My Post3",
         body: "<html><body>The contents.</body></html>",
         views: Counter.new |> Counter.increment(1),
         active: false,
         tags: Set.new |> Set.put("some_tag"),
         permalink: link
       }])

    # returned_rows = RiakDTRepo.get_by(Post, tags: Set.new |> Set.put("some_tag"))
    
    # assert row.region == returned_row.region
    # deleted_row = RiakDTRepo.delete!(row)
    # assert :deleted == deleted_row.__meta__.state
  end

  test "queries" do
    # %MySchema{
    #   region: "myregion",
    #   state: "mystate",
    #   time: 123456,
    #   weather: "sunny",
    #   temperature: 65.0
    # }
    # |> RiakDTRepo.insert!
    tags = Set.new |> Set.put("some_tag")
    query = from(e in Post,
      where: e.tags == ^tags)

    results = RiakDTRepo.all(query)
    Logger.info("Found rows by some_tag: #{inspect results}")
    # assert "myregion" == result.region
  end
end
