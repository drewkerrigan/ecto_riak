defmodule Ecto.Adapters.RiakTS.SQL do
  @moduledoc """
  Functions to convert Ecto queries to RiakTS compatible strings.

  Credit to https://github.com/elixir-ecto/ecto/blob/master/lib/ecto/adapters/mysql/connection.ex for the majority of this code.
  """

  alias Ecto.Query
  alias Ecto.Query.SelectExpr
  alias Ecto.Query.QueryExpr

  def to_sql(query) do
    sources = create_names(query)

    from     = from(query, sources)
    select   = select(query, sources)
    join     = join(query, sources)
    where    = where(query, sources)
    group_by = group_by(query, sources)
    having   = having(query, sources)
    order_by = order_by(query, sources)
    limit    = limit(query, sources)
    offset   = offset(query, sources)
    lock     = lock(query.lock)

    assemble([select, from, join, where, group_by, having, order_by, limit, offset, lock])
  end

  binary_ops =
    [==: "=", !=: "!=", <=: "<=", >=: ">=", <:  "<", >:  ">",
     and: "AND", or: "OR", like: "LIKE"]

  @binary_ops Keyword.keys(binary_ops)

  Enum.map(binary_ops, fn {op, str} ->
    defp handle_call(unquote(op), 2), do: {:binary_op, unquote(str)}
  end)

  defp handle_call(fun, _arity), do: {:fun, Atom.to_string(fun)}

  defp select(%Query{select: %SelectExpr{fields: fields}, distinct: distinct} = query,
        sources) do
    "SELECT " <>
      distinct(distinct, sources, query) <>
      select(fields, sources, query)
  end

  defp distinct(nil, _sources, _query), do: ""
  defp distinct(%QueryExpr{}, _sources, query),  do: error!(query, "Distinct is not supported by RiakTS")

  defp select([], _sources, _query),
    do: "TRUE"
  defp select(fields, sources, query),
    do: Enum.map_join(fields, ", ", &expr(&1, sources, query))

  defp from(%{from: from} = query, sources) do
    case get_source(query, sources, 0, from) do
      {from, nil} -> "FROM #{from}";
      {from, name} -> "FROM #{from} AS #{name}"
    end
  end

  defp join(%Query{joins: []}, _sources), do: []
  defp join(%Query{joins: _} = query, _sources) do
    error!(query, "RiakTS does not support joins")
  end

  defp where(%Query{wheres: wheres} = query, sources) do
    boolean("WHERE", wheres, sources, query)
  end

  defp having(%Query{havings: []}, _sources) do
    []
  end
  defp having(%Query{havings: _} = query, _sources) do
    error!(query, "RiakTS does not support HAVING")
  end

  defp group_by(%Query{group_bys: []}, _sources) do
    []
  end
  defp group_by(%Query{group_bys: _} = query, _sources) do
    error!(query, "RiakTS does not yet support GROUP BY")
  end

  defp order_by(%Query{order_bys: []}, _sources) do
    []
  end
  defp order_by(%Query{order_bys: _} = query, _sources) do
    error!(query, "RiakTS does not yet support ORDER BY")
  end

  defp limit(%Query{limit: nil}, _sources), do: []
  defp limit(%Query{limit: _} = query, _sources) do
    error!(query, "RiakTS does not yet support LIMIT")
  end

  defp offset(%Query{offset: nil}, _sources), do: []
  defp offset(%Query{offset: _} = query, _sources) do
    error!(query, "RiakTS does not yet support OFFSET")
  end

  defp lock(nil), do: []
  defp lock(_lock_clause), do: error!(nil, "RiakTS does not yet support LOCK")

  defp boolean(_name, [], _sources, _query), do: []
  defp boolean(name, query_exprs, sources, query) do
    name <> " " <>
      Enum.map_join(query_exprs, " AND ", fn
        %QueryExpr{expr: expr} ->
          "(" <> expr(expr, sources, query) <> ")"
      end)
  end

  defp expr({{:., _, [{:&, _, [idx]}, field]}, _, []}, sources, _query)
  when is_atom(field) do
    case elem(sources, idx) do
      {_, nil, _} -> "#{field}";
      {_, name, _} -> "#{name}.#{quote_name(field)}"
    end
  end

  defp expr({:&, _, [idx, fields, _counter]}, sources, query) do
    {_, name, schema} = elem(sources, idx)
    if is_nil(schema) and is_nil(fields) do
      error!(query, "RiakTS requires a schema module when using selector " <>
        "#{inspect name} but none was given. " <>
        "Please specify a schema or specify exactly which fields from " <>
        "#{inspect name} you desire")
    end
    case name do
      nil ->
        Enum.map_join(fields, ", ", &"#{quote_name(&1)}");
      n ->
        Enum.map_join(fields, ", ", &"#{n}.#{quote_name(&1)}")
    end
  end

  defp expr({:is_nil, _, [arg]}, sources, query) do
    "#{expr(arg, sources, query)} IS NULL"
  end

  defp expr({:not, _, [expr]}, sources, query) do
    "NOT (" <> expr(expr, sources, query) <> ")"
  end

  defp expr(%Ecto.SubQuery{query: query}, _sources, _query) do
    error!(query, "RiakTS does not support sub queries")
  end

  defp expr({:fragment, _, [kw]}, _sources, query) when is_list(kw) or tuple_size(kw) == 3 do
    error!(query, "RiakTS does not support keyword or interpolated fragments")
  end

  defp expr({:fragment, _, parts}, sources, query) do
    Enum.map_join(parts, "", fn
      {:raw, part}  -> part
      {:expr, expr} -> expr(expr, sources, query)
    end)
  end

  defp expr({fun, _, args}, sources, query) when is_atom(fun) and is_list(args) do
    case handle_call(fun, length(args)) do
      {:binary_op, op} ->
        [left, right] = args
        op_to_binary(left, sources, query) <>
          " #{op} "
        <> op_to_binary(right, sources, query)

      {:fun, fun} ->
        "#{fun}(" <> Enum.map_join(args, ", ", &expr(&1, sources, query)) <> ")"
    end
  end

  defp expr(%Decimal{} = decimal, _sources, _query) do
    Decimal.to_string(decimal, :normal)
  end

  defp expr(%Ecto.Query.Tagged{value: binary, type: :binary}, _sources, _query)
  when is_binary(binary) do
    hex = Base.encode16(binary, case: :lower)
    "x'#{hex}'"
  end

  defp expr(%Ecto.Query.Tagged{value: other, type: type}, sources, query)
  when type in [:id, :integer, :float] do
    expr(other, sources, query)
  end

  defp expr(nil, _sources, _query),   do: "NULL"
  defp expr(true, _sources, _query),  do: "TRUE"
  defp expr(false, _sources, _query), do: "FALSE"

  defp expr(literal, _sources, _query) when is_binary(literal) do
    "'#{escape_string(literal)}'"
  end

  defp expr(literal, _sources, _query) when is_integer(literal) do
    String.Chars.Integer.to_string(literal)
  end

  defp expr(literal, _sources, _query) when is_float(literal) do
    String.Chars.Float.to_string(literal)
  end

  defp expr(operation, _, query) do
    error!(query, "RiakTS does not support #{inspect operation}")
  end

  defp op_to_binary({op, _, [_, _]} = expr, sources, query) when op in @binary_ops do
    "(" <> expr(expr, sources, query) <> ")"
  end

  defp op_to_binary(expr, sources, query) do
    expr(expr, sources, query)
  end

  defp create_names(%{sources: sources}) do
    {table, schema} = elem(sources, 0)
    {{table, nil, schema}}
  end

  ## Helpers

  defp get_source(query, sources, ix, source) do
    {expr, name, _schema} = elem(sources, ix)
    {expr || "(" <> expr(source, sources, query) <> ")", name}
  end

  defp quote_name(name)
  defp quote_name(name) when is_atom(name),
    do: quote_name(Atom.to_string(name))
  defp quote_name(name) do
    if String.contains?(name, "`") do
      error!(nil, "bad field name #{inspect name}")
    end
    name
    # RiakTS does not require column names to be quoted
    # "'" <> name <> "'"
  end

  defp assemble(list), do: assemble(list, " ")
  defp assemble(list, joiner) do
    list
    |> List.flatten
    |> Enum.reject(fn(v)-> v == "" end)
    |> Enum.join(joiner)
  end

  defp escape_string(value) when is_binary(value) do
    value
    |> :binary.replace("'", "''", [:global])
    |> :binary.replace("\\", "\\\\", [:global])
  end

  defp error!(nil, message) do
    raise ArgumentError, message
  end
  defp error!(query, message) do
    raise Ecto.QueryError, query: query, message: message
  end
end
