defmodule Ecto.Adapters.RiakTS do
  @moduledoc """
  Adapter module for RiakTS.
  """

  require Logger

  import Ecto.Adapters.RiakTS.SQL, only: [to_sql: 1]

  @behaviour Ecto.Adapter

  defmacro __before_compile__(_env) do
    :ok
  end

  def ensure_all_started(_, _options) do
    {:ok, []}
  end

  def child_spec(_repo, options) do
    Ecto.Adapters.RiakTS.Connection.child_spec(options)
  end

  ## Types

  def loaders(:binary_id, type), do: [Ecto.UUID, type]
  def loaders(_primitive, type), do: [type]

  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_primitive, type), do: [type]

  def autogenerate(:id), do: nil
  def autogenerate(:embed_id), do: Ecto.UUID.autogenerate
  def autogenerate(:binary_id), do: Ecto.UUID.autogenerate

  ## Queryable

  def prepare(operation, query), do: {:nocache, {operation, query}}

  def execute(_repo,
        %{fields: fields},
        {:nocache, {:all, query}}, [], process, _options) do
    sql = to_sql(query)
    case Riak.Timeseries.query(sql) do
      {_fields, []} -> {0, []};
      {:error, {_, message}} ->
        raise Ecto.QueryError, query: query, message: message <> " Generated SQL: '#{sql}'"
      {_fields, raw_rows} ->
        {rows, count} = Enum.map_reduce(raw_rows, 0, &{process_row(&1, process, fields), &2 + 1})
        {count, rows}
    end
  end

  def execute(_repo,
        %{fields: fields,
          sources: {{table, schema}}},
        {:nocache, {:all, _query}}, params, process, _options) do
    pk = schema.__schema__(:primary_key)
    case length(params) == length(pk) do
      false ->
        raise ArgumentError, "RiakTS only supports gets with a full primary key (#{inspect pk})";
      true ->
        case Riak.Timeseries.get(table, params) do
          {_fields, []} -> {0, []};
          {:error, {_, message}} ->
            raise ArgumentError, message;
          {_fields, raw_rows} ->
            {rows, count} = Enum.map_reduce(raw_rows, 0, &{process_row(&1, process, fields), &2 + 1})
            {count, rows}
        end
    end
  end

  def execute(_, _, _, _, _) do
    {:error, "Not implemented"}
  end

  ## Schema

  def insert(_repo, %{source: {_, table}, schema: schema}, row, _returning, _options) do
    fields = schema.__schema__(:fields)
    tuple_row = convert_row(fields, row)
    case Riak.Timeseries.put(table, [tuple_row]) do
      :ok -> {:ok, []};
      {:error, {_, message}} ->
        raise ArgumentError, message
    end
  end

  def insert_all(_repo, %{source: {_, table}, schema: schema},
        _header, rows, returning, _options) do
    fields = schema.__schema__(:fields)
    tuple_rows = Enum.map rows, fn r -> convert_row(fields, r) end
    case Riak.Timeseries.put(table, tuple_rows) do
      :ok ->
        case returning do
          [] ->
            {length(rows), nil}
          return_fields ->
            return_values = Enum.map rows, fn r ->
              Enum.map return_fields, fn f ->
                {f, r[f]}
              end
            end
            {length(rows), return_values}
        end
      {:error, {_, message}} -> {:invalid, message}
    end
  end

  # Notice the list of changes is never empty.
  def update(_repo, %{context: nil}, [_|_], _filters, return, _options),
    do: send(self(), :update) && {:ok, Enum.zip(return, 1..length(return))}

  def update(_repo, %{context: {:invalid, _}=res}, [_|_], _filters, _return, _options),
    do: res

  def delete(_repo, %{source: {_, table}, schema: schema}, filter, _options) do
    pk = schema.__schema__(:primary_key)
    case to_pk(pk, filter, []) do
      {:invalid, _} = e -> e;
      key ->
        case Riak.Timeseries.delete(table, key) do
          :ok -> {:ok, []};
          {:error, {_, "notfound"}} -> {:error, :stale};
          {:error, {_, message}} ->
            raise ArgumentError, message
        end
    end
  end

  ## Private

  defp process_row(row_tuple, process, fields) do
    row = :erlang.tuple_to_list(row_tuple)
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

  defp convert_row(fields, row) do
    :erlang.list_to_tuple(
      Enum.map fields, fn f ->
        row[f]
      end)
  end

  defp to_pk([], _fields, acc) do
    Enum.reverse(acc)
  end

  defp to_pk([name|rest], fields, acc) do
    case Keyword.get(fields, name) do
      nil -> {:invalid, [{:required, name}]}
      val -> to_pk(rest, fields, [val | acc])
    end
  end
end
