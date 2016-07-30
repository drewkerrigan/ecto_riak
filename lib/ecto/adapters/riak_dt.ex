defmodule Ecto.Adapters.RiakDT do
  @moduledoc """
  Adapter module for Riak (KV).
  """

  require Logger
  require Record

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

  def loaders(:binary_id, type), do: loaders(:string, type)
  def loaders(:string, _type) do
    [fn
      rec when Record.is_record(rec, :register) ->
      case :riakc_register.to_op(rec) do
        :undefined ->
          {:ok, Riak.CRDT.Register.value(rec)}
        {_, {:assign, v}, _} ->
          {:ok, v}
      end
      data ->
        {:ok, data}
    end]
  end
  def loaders({:array, :string}, _type) do
    [fn
      rec when Record.is_record(rec, :set) ->
        {:ok, Riak.CRDT.Set.value(rec)}
      data ->
        {:ok, data}
    end]
  end
  def loaders(:boolean, _type) do
    [fn
      rec when Record.is_record(rec, :flag) ->
        {:ok, Riak.CRDT.Flag.value(rec)}
      data ->
        {:ok, data}
    end]
  end
  def loaders({:embed, %{related: schema}}, type) do
    types = schema.__schema__(:types)
    [fn
      rec when Record.is_record(rec, :map) ->
        list = Enum.map(Riak.CRDT.Map.value(rec), fn
          {{k_str, :counter}, v_rec} ->
            {String.to_atom(k_str), :riakc_counter.new(v_rec, :undefined)}
          {{k_str, :map}, v_rec} ->
            {String.to_atom(k_str), :riakc_map.new(v_rec, :undefined)}
          {{k_str, _}, v_rec} ->
            {String.to_atom(k_str), v_rec}
        end)
      data = Enum.reduce(types, %{}, fn ({k, t}, a) ->
        [f|_] = loaders(t, t)
        raw = Keyword.get(list, k)
        case f do
          Ecto.Riak.Counter ->
            Map.put(a, Atom.to_string(k), raw)
          fun ->
            {:ok, v} = f.(raw)
            Map.put(a, Atom.to_string(k), v)
        end
      end)
      {:ok, data}
      data ->
        {:ok, data}
    end, &load_embed(type, &1)]
  end
  def loaders(_primitive, type), do: [type]

  def load_embed(type, value) do
    Ecto.Type.load(type, value, fn
      {:embed, _} = type, value -> load_embed(type, value)
      type, value -> Ecto.Type.cast(type, value)
    end)
  end

  def dumpers(:binary_id, type), do: dumpers(:string, type)
  def dumpers(:string, _type) do
    [fn
      rec when Record.is_record(rec, :register) ->
        {:ok, rec}
      data ->
        {:ok, Riak.CRDT.Register.new |> Riak.CRDT.Register.set(data)}
    end]
  end
  def dumpers({:array, :string}, _type) do
    [fn
      rec when Record.is_record(rec, :set) ->
        {:ok, rec}
      data ->
        o = Riak.CRDT.Set.new
        {:ok, Enum.reduce(data, o, fn d, a -> Riak.CRDT.Set.put(a, d) end)}
    end]
  end
  def dumpers(:boolean, _type) do
    [fn
      rec when Record.is_record(rec, :flag) ->
        {:ok, rec}
      true ->
        {:ok, Riak.CRDT.Flag.new |> Riak.CRDT.Flag.enable}
      false ->
        {:ok, Riak.CRDT.Flag.new |> Riak.CRDT.Flag.disable}
    end]
  end
  def dumpers({:embed, %{related: schema}}, _type) do
    types = schema.__schema__(:types)
    [fn
      rec when Record.is_record(rec, :map) ->
        {:ok, rec}
      data ->
        data = Enum.reduce(types, Riak.CRDT.Map.new, fn ({k, t}, a) ->
      [f] = dumpers(t, t)
      {:ok, raw} = Map.fetch(data, k)
      case f do
        Ecto.Riak.Counter ->
          Riak.CRDT.Map.put(a, Atom.to_string(k), raw)
        fun ->
          {:ok, v} = f.(raw)
          Riak.CRDT.Map.put(a, Atom.to_string(k), v)
      end
    end)
      {:ok, data}
    end]
  end
  def dumpers(_primitive, type), do: [type]

  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.autogenerate
  def autogenerate(:binary_id), do: Ecto.UUID.autogenerate

  ## Queryable

  def prepare(operation, query), do: {:nocache, {operation, query}}

  def execute(_repo,
        %{fields: fields},
        {:nocache, {:all, query}}, [], process, _options) do
    Logger.info("Execute query")
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

  def execute(_repo,
        %{fields: fields,
          sources: {{bucket, schema}}},
        {:nocache, {:all, _query}}, params, process, _options) do
    Logger.info("Execute get_by, params: #{inspect params}")
    {type, bucket} = List.to_tuple(String.split(bucket, "."))
    [f] = loaders(:binary_id, nil)
    [id_reg] = params
    {:ok, id} = f.(id_reg)
    case Riak.find(type, bucket, id) do
      {:error, message} ->
        raise ArgumentError, message;
      map ->
        [f|_] = loaders({:embed, %{related: schema}}, nil)
        {:ok, row} = f.(map)
        Logger.info("Fields: #{inspect fields}")
        ordered_fields = schema.__schema__(:fields)
        values = Enum.map(ordered_fields, fn f ->
          Map.get(row, Atom.to_string(f))
        end)
        Logger.info("Values: #{inspect values}")
        {rows, count} = Enum.map_reduce([values], 0, &{process_row(&1, process, fields), &2 + 1})
        {count, rows}
    end
  end

  def execute(_, _, _, _, _) do
    {:error, "Not implemented"}
  end

  ## Schema

  def insert(_repo, %{source: {_, bucket}, schema: schema}, params, _returning, _options) do
    Logger.info("Insert, #{inspect params}")
    # m = to_crdt_map(params)
    # Logger.info("New Map, #{inspect m}")
    {type, bucket} = List.to_tuple(String.split(bucket, "."))
    [f] = dumpers({:embed, %{related: schema}}, nil)
    {:ok, map} = f.(Enum.into(params, %{}))
    case Riak.update(map, type, bucket, Keyword.get(params, :id)) do
      :ok -> {:ok, []};
      {:error, message} ->
        raise ArgumentError, message
    end
  end

  def insert_all(_repo, %{source: {_, table}, schema: schema},
        _header, rows, returning, _options) do
    Logger.info("Insert all")
    # fields = schema.__schema__(:fields)
    # tuple_rows = Enum.map rows, fn r -> convert_row(fields, r) end
    # case Riak.Timeseries.put(table, tuple_rows) do
    #   :ok ->
    #     case returning do
    #       [] ->
    #         {length(rows), nil}
    #       return_fields ->
    #         return_values = Enum.map rows, fn r ->
    #           Enum.map return_fields, fn f ->
    #             {f, r[f]}
    #           end
    #         end
    #         {length(rows), return_values}
    #     end
    #   {:error, {_, message}} -> {:invalid, message}
    # end
    {0, nil}
  end

  # Notice the list of changes is never empty.
  def update(_repo, %{context: nil}, [_|_], _filters, return, _options) do
    Logger.info("Update")
    # do: send(self(), :update) && {:ok, Enum.zip(return, 1..length(return))}
  end

  def update(_repo, %{context: {:invalid, _}=res}, [_|_], _filters, _return, _options) do
    Logger.info("Update res")
    # do: res
  end

  def delete(_repo, %{source: {_, table}, schema: schema}, filter, _options) do
    Logger.info("Delete")
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

  defp process_row(row, process, fields) do
    Enum.map_reduce(fields, row, fn
      {:&, _, [_, _, counter]} = field, acc ->
        case split_and_not_nil(acc, counter, true, []) do
          {nil, rest} -> {nil, rest}
          {val, rest} -> {process.(field, val, nil), rest}
        end
      field, [h|t] ->
        {process.(field, h, nil), t}
    end) |> elem(0)
  end

  defp split_and_not_nil(rest, 0, true, _acc), do: {nil, rest}
  defp split_and_not_nil(rest, 0, false, acc), do: {:lists.reverse(acc), rest}

  defp split_and_not_nil([nil|t], count, all_nil?, acc) do
    split_and_not_nil(t, count - 1, all_nil?, [nil|acc])
  end

  defp split_and_not_nil([h|t], count, _all_nil?, acc) do
    split_and_not_nil(t, count - 1, false, [h|acc])
  end
end
