defmodule Ecto.Riak.Counter do
  @behaviour Ecto.Type
  alias Riak.CRDT.Counter
  require Record
  def type, do: Record
  def cast(value) when Record.is_record(value, :counter), do: {:ok, value}
  def cast(_), do: :error
  def load(value) when Record.is_record(value, :counter), do: {:ok, value}
  def load(_), do: :error
  def dump(value) when Record.is_record(value, :counter), do: {:ok, value}
  def dump(_), do: :error
  def new, do: Counter.new
  def increment(counter, amount), do: Counter.increment(counter, amount)
  def decrement(counter, amount), do: Counter.decrement(counter, amount)
  def value(counter), do: Counter.value(counter)
end

defmodule Ecto.Riak.Flag do
  @behaviour Ecto.Type
  alias Riak.CRDT.Flag
  require Record
  def type, do: Record
  def cast(value) when Record.is_record(value, :flag), do: {:ok, value}
  def cast(_), do: :error
  def dump(value) when Record.is_record(value, :flag), do: {:ok, value}
  def dump(_), do: :error
  def load(value) when Record.is_record(value, :flag), do: {:ok, value}
  def load(_), do: :error
  def new, do: Flag.new
  def enable(flag), do: Flag.enable(flag)
  def disable(flag), do: Flag.disable(flag)
  def value(flag), do: Flag.value(flag)
end

defmodule Ecto.Riak.Map do
  @behaviour Ecto.Type
  alias Riak.CRDT.Map
  require Record
  def type, do: Record
  def cast(value) when Record.is_record(value, :map), do: {:ok, value}
  def cast(_), do: :error
  def dump(value) when Record.is_record(value, :map), do: {:ok, value}
  def dump(_), do: :error
  def load(value) when Record.is_record(value, :map), do: {:ok, value}
  def load(_), do: :error
  def new, do: Map.new
  def size(map), do: Map.size(map)
  def get(map, key_type, key), do: Map.get(map, riak_type(key_type), key)
  def update(map, key_type, key, fun), do: Map.update(map, riak_type(key_type), key, fun)
  def put(map, key, value), do: Map.put(map, key, value)
  def delete(map, key_type, key), do: Map.delete(map, {riak_type(key_type), key})
  def value(map), do: Map.value(map)
  def keys(map), do: Map.keys(map)
  def has_key?(map, key_type, key), do: Map.has_key?(map, {riak_type(key_type), key})
  defp riak_type(Ecto.Riak.Register), do: :register
  defp riak_type(Ecto.Riak.Counter), do: :counter
  defp riak_type(Ecto.Riak.Flag), do: :flag
  defp riak_type(Ecto.Riak.Map), do: :map
  defp riak_type(Ecto.Riak.Set), do: :set
  defp riak_type(type) when is_atom(type), do: type
end

defmodule Ecto.Riak.Register do
  @behaviour Ecto.Type
  alias Riak.CRDT.Register
  require Record
  def type, do: Record
  def cast(value) when Record.is_record(value, :register), do: {:ok, value}
  def cast(_), do: :error
  def dump(value) when Record.is_record(value, :register), do: {:ok, value}
  def dump(_), do: :error
  def load(value) when Record.is_record(value, :register), do: {:ok, value}
  def load(_), do: :error
  def new, do: Register.new
  def set(register, value), do: Register.set(register, value)
  def value(register), do: Register.value(register)
end

defmodule Ecto.Riak.Set do
  @behaviour Ecto.Type
  alias Riak.CRDT.Set
  require Record
  def type, do: Record
  def cast(value) when Record.is_record(value, :set), do: {:ok, value}
  def cast(_), do: :error
  def dump(value) when Record.is_record(value, :set), do: {:ok, value}
  def dump(_), do: :error
  def load(value) when Record.is_record(value, :set), do: {:ok, value}
  def load(_), do: :error
  def new, do: Set.new
  def member?(set, value), do: Set.member?(set, value)
  def put(set, value), do: Set.put(set, value)
  def delete(set, value), do: Set.delete(set, value)
  def size(set), do: Set.size(set)
  def value(set), do: Set.value(set)
end
