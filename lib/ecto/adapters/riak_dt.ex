defmodule Ecto.Adapters.RiakDT do
  @moduledoc """
  Adapter module for Riak (KV).
  """

  require Record
  require Logger

  @behaviour Ecto.Adapter

  defmacro __before_compile__(_env) do
    :ok
  end

  def ensure_all_started(_, _options) do
    {:ok, []}
  end

  def child_spec(_repo, options) do
    Ecto.Riak.Connection.child_spec(options)
  end

  ## Types

  def loaders(_primitive, type), do: [type]

  def dumpers(_primitive, type), do: [type]

  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.autogenerate
  def autogenerate(:binary_id), do: Ecto.UUID.autogenerate
  def autogenerate(any), do: "hello"

  ## Queryable

  def prepare(operation, query), do: {:nocache, {operation, query}}

  def execute(_repo,
        %{fields: fields},
        {:nocache, {:all, query}}, [], process, _options) do
    Logger.info("all query: #{inspect query}")
    # case Riak.Timeseries.query(sql) do
    #   {_fields, []} -> {0, []};
    #   {:error, {_, message}} ->
    #     raise Ecto.QueryError, query: query, message: message <> " Generated SQL: '#{sql}'"
    #   {_fields, raw_rows} ->
    #     {rows, count} = Enum.map_reduce(raw_rows, 0, &{process_row(&1, process, fields), &2 + 1})
    #     {count, rows}
    # end
    {0, nil}
  end

  def execute(repo, meta,{:nocache,{:all,query}}=q, params, process, options) do
    case simple_where(query, params) do
      {:id, id} -> get_by_id(repo, meta, q, params, process, options)
      _ -> get_by_term(repo, meta, q, params, process, options)
    end
  end

  def execute(_, _, _, _, _) do
    {:error, "Not implemented"}
  end

  def get_by_id(_repo,
        %{fields: fields,
          sources: {{bucket, schema}}},
        {:nocache, {:all, _query}}, [id], process, _options) do
    Logger.info("Get By Id: #{inspect id}")
    {type, bucket} = List.to_tuple(String.split(bucket, "."))
    case Riak.find(type, bucket, id) do
      {:error, message} ->
        raise ArgumentError, message;
      map ->
        ordered_fields = schema.__schema__(:fields)
        types = schema.__schema__(:types)
        row = from_crdt_map(map, types, ordered_fields)
        values = Keyword.values(row)
        {rows, count} = Enum.map_reduce(
          [values], 0,
          &{process_row(&1, process, fields, map), &2 + 1})
        {count, rows}
    end
  end

  def get_by_term(repo, %{sources: {{_, schema}}}=meta,
        {:nocache, {:all, query}}=q, params,
        process, options) do
    {k, v} = simple_where(query, params)
    Logger.info("Query by #{inspect k}, #{inspect v}")
    term_bucket = Keyword.get(schema.term_indexes, k)
    types = schema.__schema__(:types)
    {type, bucket} = List.to_tuple(String.split(term_bucket, "."))
    id = to_term_key(v, Map.get(types, k))
    Logger.info("Term Key #{id}")
    case Riak.find(type, bucket, id) do
      {:error, message} ->
        raise ArgumentError, message;
      set ->
        ids = Riak.CRDT.Set.value(set)
        {c, r} = Enum.reduce(ids, {0, []}, fn id, {c0, r0} ->
          {count, rows} = get_by_id(repo, meta, q, [id], process, options)
          {count + c0, r0 ++ rows}
        end)
        {c, r}
    end
  end

  def simple_where(
        %Ecto.Query{
          wheres:
          [%Ecto.Query.QueryExpr{
              expr: {:==, _, [{{_, _, [_, k]}, _, _}, {:^, [], [0]}]}}]}, [v]) do
    {k, v}
  end
  def simple_where(_, [v]) do
    {:id, v}
  end

  def to_term_key(set, Ecto.Riak.Set) do
    case :riakc_set.to_op(set) do
      :undefined -> Riak.CRDT.Set.value(set)
      {_, {:add_all, [id]}, _} -> id
      {_, {:update, [{_, _}, {:add_all, [id]}]}, _} -> id
    end
  end

  ## Schema

  def build_index(bucket, items, Ecto.Riak.Set, id) do
    {type, bucket} = List.to_tuple(String.split(bucket, "."))
    Enum.reduce(Riak.CRDT.Set.value(items), :ok, fn (i, a) ->

      set = case Riak.find(type, bucket, i) do
              set when Record.is_record(:set) ->
                set
              _ ->
                Riak.CRDT.Set.new
            end

      case Riak.CRDT.Set.put(set, id) |> Riak.update(type, bucket, i) do
        :ok -> a
        e -> e
      end
    end)
  end

  def insert(_repo,
        %{source: {_, bucket},
          schema: schema,
          context: context}, params, _returning, _options) do
    types = schema.__schema__(:types)
    map = to_crdt_map(params, types, context)
    term_indexes = schema.term_indexes
    id = Keyword.get(params, :id)

    {type, bucket} = List.to_tuple(String.split(bucket, "."))
    case Riak.update(map, type, bucket, id) do
      :ok ->
        case term_indexes do
          [] ->
            {:ok, []};
          [_|_] ->
            Enum.reduce(term_indexes, {:ok, []}, fn {n, b}, a ->
              case build_index(b, Keyword.get(params, n), Map.get(types, n), id) do
                :ok -> a
                {:error, message} ->
                  raise ArgumentError, message
              end
            end)
        end
      {:error, message} ->
        raise ArgumentError, message
    end
  end

  def insert_all(repo, meta,
        _header, rows, returning, options) do
    Logger.info("Insert All: #{inspect rows}")
    Enum.reduce(rows, {0, []}, fn r, {c, a} ->
      case insert(repo, meta, r, returning, options) do
        {:ok, _} -> {c+1, nil};
        _ -> {c, nil}
      end
    end)
  end

  # Notice the list of changes is never empty.
  def update(_repo, %{context: nil}, [_|_], _filters, return, _options) do
    # do: send(self(), :update) && {:ok, Enum.zip(return, 1..length(return))}
  end

  def update(_repo, %{context: {:invalid, _}=res}, [_|_], _filters, _return, _options) do
    # do: res
  end

  def delete(_repo, %{source: {_, table}, schema: schema}, filter, _options) do
    {:ok, []}
    # pk = schema.__schema__(:primary_key)
    # case to_pk(pk, filter, []) do
    #   {:invalid, _} = e -> e;
    #   key ->
    #     case Riak.Timeseries.delete(table, key) do
    #       :ok -> {:ok, []};
    #       {:error, {_, "notfound"}} -> {:error, :stale};
    #       {:error, {_, message}} ->
    #         raise ArgumentError, message
    #     end
    # end
  end

  ## Private

  defp process_row(row, process, fields, context) do
    [h|_] = Enum.map_reduce(fields, row, fn
      {:&, _, [_, _, counter]} = field, acc ->
        case split_and_not_nil(acc, counter, true, []) do
          {nil, rest} -> {nil, rest}
          {val, rest} -> {process.(field, val, nil), rest}
        end
      field, [h|t] ->
        {process.(field, h, nil), t}
    end) |> elem(0)
    [h |> Ecto.put_meta(context: %{map: context})]
  end

  defp split_and_not_nil(rest, 0, true, _acc), do: {nil, rest}
  defp split_and_not_nil(rest, 0, false, acc), do: {:lists.reverse(acc), rest}

  defp split_and_not_nil([nil|t], count, all_nil?, acc) do
    split_and_not_nil(t, count - 1, all_nil?, [nil|acc])
  end

  defp split_and_not_nil([h|t], count, _all_nil?, acc) do
    split_and_not_nil(t, count - 1, false, [h|acc])
  end

  # TODO: Move to another module
  def from_crdt_map(map, types, fields) when Record.is_record(map, :map) do
    Riak.CRDT.Map.value(map) |> from_crdt_map(types, fields)
  end
  def from_crdt_map(map_vals, types, fields) do
    list = Enum.map(map_vals,
      fn {{ck, ct}=crdt_key, v} ->
        t = Map.get(types, String.to_atom(ck))
        {crdt_key, t, v}
      end)
    |> from_crdt_map([])
    Enum.map(fields, fn f ->
      {f, Keyword.get(list, f)}
    end)
  end

  def from_crdt_map([], acc) do
    Enum.reverse(acc)
  end
  def from_crdt_map([{{k, :register}, _, v}|rest], acc) do
    # from_crdt_map(rest, [{k, Riak.CRDT.Register.value(v)}|acc])
    from_crdt_map(rest, [{String.to_atom(k), v}|acc])
  end
  def from_crdt_map([{{k, :flag}, _, v}|rest], acc) do
    from_crdt_map(rest, [{String.to_atom(k), v}|acc])
  end
  def from_crdt_map([{{k, :counter}, Ecto.Riak.Counter, v}|rest], acc) do
    from_crdt_map(rest, [{String.to_atom(k),
                          :riakc_counter.new(v, :undefined)}|acc])
  end
  def from_crdt_map([{{k, :set}, Ecto.Riak.Set, v}|rest], acc) do
    from_crdt_map(rest, [{String.to_atom(k),
                          :riakc_set.new(v, :undefined)}|acc])
  end
  def from_crdt_map([{{k, :map}, {:embed, %{related: schema}}, v}|rest], acc) do
    fields = schema.__schema__(:fields)
    types = schema.__schema__(:types)
    sub_map = from_crdt_map(v, types, fields)
    |> Enum.map(fn {ik, iv} -> {Atom.to_string(ik), iv} end)
    |> Enum.into(%{})
    from_crdt_map(rest, [{String.to_atom(k), sub_map}|acc])
  end

  def to_crdt_key(k, :binary_id), do: {Atom.to_string(k), :register}
  def to_crdt_key(k, :string), do: {Atom.to_string(k), :register}
  def to_crdt_key(k, :boolean), do: {Atom.to_string(k), :flag}
  def to_crdt_key(k, Ecto.Riak.Counter), do: {Atom.to_string(k), :counter}
  def to_crdt_key(k, Ecto.Riak.Set), do: {Atom.to_string(k), :set}
  def to_crdt_key(k, {:embed, _}), do: {Atom.to_string(k), :map}

  def to_crdt_map(%{}=params, types, context) do
    to_crdt_map(Map.to_list(params), types, context)
  end
  def to_crdt_map(params, types, %{map: map}) do
    Enum.map(types, fn {k, t} ->
      {ck, ct} = crdt_key = to_crdt_key(k, t)
      old = Riak.CRDT.Map.get(map, ct, ck)
      new = Keyword.get(params, k)
      {crdt_key, t, old, new}
    end) |> to_crdt_map(map)
  end
  def to_crdt_map(params, types, _) do
    Enum.map(types, fn {k, t} ->
      crdt_key = to_crdt_key(k, t)
      {crdt_key, t, Keyword.get(params, k)}
    end) |> to_crdt_map(Riak.CRDT.Map.new)
  end

  def to_crdt_map([], map) do
    map
  end
  def to_crdt_map([{{k, :register}, _, nil}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k,
        Riak.CRDT.Register.new
        |> Riak.CRDT.Register.set("")))
  end
  def to_crdt_map([{{k, :register}, _, v}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k,
        Riak.CRDT.Register.new
        |> Riak.CRDT.Register.set(v)))
  end
  def to_crdt_map([{{_, :register}, _, old, old}|rest], map) do
    to_crdt_map(rest, map)
  end
  def to_crdt_map([{{k, :register=t}, _, old, new}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k, :riakc_register.new(old, :undefined)
      |> Riak.CRDT.Register.set(new)))
  end
  def to_crdt_map([{{k, :flag}, _, true}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k,
        Riak.CRDT.Flag.new
        |> Riak.CRDT.Flag.enable))
  end
  def to_crdt_map([{{k, :flag}, _, false}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k,
        Riak.CRDT.Flag.new
        |> Riak.CRDT.Flag.disable))
  end
  def to_crdt_map([{{_, :flag}, _, old, old}|rest], map) do
    to_crdt_map(rest, map)
  end
  def to_crdt_map([{{k, :flag=t}, _, old, true}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k, :riakc_flag.new(old, :undefined)
      |> Riak.CRDT.Flag.enable))
  end
  def to_crdt_map([{{k, :flag=t}, _, old, false}|rest], map) do
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k, :riakc_flag.new(old, :undefined)
      |> Riak.CRDT.Flag.disable))
  end
  def to_crdt_map([{{k, :set}, _, v}|rest], map) do
    to_crdt_map(rest, Riak.CRDT.Map.put(map, k, v))
  end
  def to_crdt_map([{{k, :set}, et, _old, new}|rest], map) do
    to_crdt_map([{{k, :set}, et, new}|rest], map)
  end
  def to_crdt_map([{{k, :counter}, _, v}|rest], map) do
    to_crdt_map(rest, Riak.CRDT.Map.put(map, k, v))
  end
  def to_crdt_map([{{k, :counter}, et, _old, new}|rest], map) do
    to_crdt_map([{{k, :counter}, et, new}|rest], map)
  end
  def to_crdt_map([{{k, :map}, {:embed, %{related: schema}},
                    v}|rest], map) do
    types = schema.__schema__(:types)
    to_crdt_map(rest, Riak.CRDT.Map.put(map, k,
          to_crdt_map(v, types, nil)))
  end
  def to_crdt_map([{{k, :map},
                    {:embed, %{related: schema}=embed}, old, new}|rest], map) do
    types = schema.__schema__(:types)
    to_crdt_map(rest,
      Riak.CRDT.Map.put(map, k,
        to_crdt_map(new, types,
          %{map: :riakc_map.new(old, :undefined)})))
  end
end
