defmodule Ecto.Riak.Counter do
  @behaviour Ecto.Type
  alias Riak.CRDT.Counter
  defstruct counter: Counter.new
  @type t :: %__MODULE__{counter: :any}
  def type, do: :any
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error
  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def increment(%__MODULE__{counter: counter}, amount),
    do: %__MODULE__{counter: Counter.increment(counter, amount)}

  def decrement(%__MODULE__{counter: counter}, amount),
    do: %__MODULE__{counter: Counter.decrement(counter, amount)}

  def value(%__MODULE__{counter: counter}),
    do: %__MODULE__{counter: Counter.value(counter)}

  def crdt(%__MODULE__{counter: counter}), do: counter
end

defmodule Ecto.Riak.Flag do
  @behaviour Ecto.Type
  alias Riak.CRDT.Flag
  defstruct flag: Flag.new
  @type t :: %__MODULE__{flag: :any}
  def type, do: :any
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error
  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def enable(%__MODULE__{flag: flag}),
    do: %__MODULE__{flag: Flag.enable(flag)}

  def disable(%__MODULE__{flag: flag}),
    do: %__MODULE__{flag: Flag.disable(flag)}

  def value(%__MODULE__{flag: flag}),
    do: %__MODULE__{flag: Flag.value(flag)}

  def crdt(%__MODULE__{flag: flag}), do: flag
end

defmodule Ecto.Riak.Map do
  @behaviour Ecto.Type
  alias Riak.CRDT.Map
  defstruct map: Map.new
  @type t :: %__MODULE__{map: :any}
  def type, do: :any
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error
  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def size(%__MODULE__{map: map}),
    do: %__MODULE__{map: Map.size(map)}

  def get(%__MODULE__{map: map}, key_type, key),
    do: %__MODULE__{map: Map.get(map, riak_type(key_type), key)}

  def update(%__MODULE__{map: map}, key_type, key, fun),
    do: %__MODULE__{map: Map.update(map, riak_type(key_type), key, fun)}

  def put(%__MODULE__{map: map}, key, value) do
    crdt_value =
      case value.__struct__ do
        Ecto.Riak.Register -> value.register;
        Ecto.Riak.Counter -> value.counter;
        Ecto.Riak.Flag -> value.flag;
        Ecto.Riak.Map -> value.map;
        Ecto.Riak.Set -> value.set
    end
    %__MODULE__{map: Map.put(map, key, crdt_value)}
  end

  def delete(%__MODULE__{map: map}, key_type, key),
    do: %__MODULE__{map: Map.delete(map, {riak_type(key_type), key})}

  def value(%__MODULE__{map: map}),
    do: %__MODULE__{map: Map.value(map)}

  def keys(%__MODULE__{map: map}),
    do: %__MODULE__{map: Map.keys(map)}

  def has_key?(%__MODULE__{map: map}, key_type, key),
    do: %__MODULE__{map: Map.has_key?(map, {riak_type(key_type), key})}

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
  defstruct register: Register.new
  @type t :: %__MODULE__{register: :any}
  def type, do: :any
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error
  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def set(%__MODULE__{register: register}, value),
    do: %__MODULE__{register: Register.set(register, value)}

  def value(%__MODULE__{register: register}),
    do: %__MODULE__{register: Register.value(register)}
end

defmodule Ecto.Riak.Set do
  @behaviour Ecto.Type
  alias Riak.CRDT.Set
  defstruct set: Set.new
  @type t :: %__MODULE__{set: :any}
  def type, do: :any
  def cast(%__MODULE__{} = value), do: {:ok, value}
  def cast(_), do: :error
  def dump(%__MODULE__{} = value), do: {:ok, value}
  def dump(_), do: :error
  def load(%__MODULE__{} = value), do: {:ok, value}
  def load(_), do: :error

  def member?(%__MODULE__{set: set}, value),
    do: %__MODULE__{set: Set.member?(set, value)}

  def put(%__MODULE__{set: set}, value),
    do: %__MODULE__{set: Set.put(set, value)}

  def delete(%__MODULE__{set: set}, value),
    do: %__MODULE__{set: Set.delete(set, value)}

  def size(%__MODULE__{set: set}),
    do: %__MODULE__{set: Set.size(set)}

  def value(%__MODULE__{set: set}),
    do: %__MODULE__{set: Set.value(set)}
end
