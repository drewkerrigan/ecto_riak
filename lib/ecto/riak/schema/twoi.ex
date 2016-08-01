defmodule Ecto.Riak.Schema do
  @doc false
  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema
      import Ecto.Riak.Schema

      @term_indexes []

      @before_compile Ecto.Riak.Schema
    end
  end

  defmacro term_index(name, bucket) do
    quote do
      @term_indexes [{unquote(name), unquote(bucket)} | @term_indexes]
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    quote do
      def term_indexes do
        @term_indexes
      end
    end
  end
end


# defmodule Ecto.Riak.Schema.TwoI do

#   require Logger

#   defmacro __using__(_) do
#     quote do
#       use Ecto.Schema
#       import Ecto.Riak.Schema.TwoI, only: [indexed_field: 3]
#     end
#   end
# end

# defmodule Ecto.Riak.Schema do
#     @type t :: %{__struct__: atom}

#   defmodule Metadata do
#     defstruct [:state, :source, :context]

#     defimpl Inspect do
#       import Inspect.Algebra

#       def inspect(metadata, opts) do
#         %{source: {prefix, source}, state: state, context: context} = metadata
#         entries =
#           for entry <- [state, prefix, source, context],
#               entry != nil,
#               do: to_doc(entry, opts)
#         concat ["#Ecto.Schema.Metadata<"] ++ Enum.intersperse(entries, ", ") ++ [">"]
#       end
#     end
#   end

#   @doc false
#   defmacro __using__(_) do
#     quote do
#       import Ecto.Schema, only: [schema: 2, embedded_schema: 1]

#       @primary_key nil
#       @timestamps_opts []
#       @foreign_key_type :id
#       @schema_prefix nil

#       Module.register_attribute(__MODULE__, :ecto_primary_keys, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_fields, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_assocs, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_embeds, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_raw, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_autogenerate, accumulate: true)
#       Module.register_attribute(__MODULE__, :ecto_autoupdate, accumulate: true)
#       Module.put_attribute(__MODULE__, :ecto_autogenerate_id, nil)
#     end
#   end

#   defmacro embedded_schema([do: block]) do
#     schema(nil, false, :binary_id, block)
#   end

#   defmacro schema(source, [do: block]) do
#     schema(source, true, :id, block)
#   end

#   defp schema(source, meta?, type, block) do
#     quote do
#       Module.register_attribute(__MODULE__, :changeset_fields, accumulate: true)
#       Module.register_attribute(__MODULE__, :struct_fields, accumulate: true)

#       meta?  = unquote(meta?)
#       source = unquote(source)
#       prefix = Module.get_attribute(__MODULE__, :schema_prefix)

#       if meta? do
#         unless is_binary(source) do
#           raise ArgumentError, "schema source must be a string, got: #{inspect source}"
#         end

#         Module.put_attribute(__MODULE__, :struct_fields,
#                              {:__meta__, %Metadata{state: :built, source: {prefix, source}}})
#       end

#       if @primary_key == nil do
#         @primary_key {:id, unquote(type), autogenerate: true}
#       end

#       primary_key_fields =
#         case @primary_key do
#           false ->
#             []
#           {name, type, opts} ->
#             Ecto.Schema.__field__(__MODULE__, name, type, [primary_key: true] ++ opts)
#             [name]
#           other ->
#             raise ArgumentError, "@primary_key must be false or {name, type, opts}"
#         end

#       try do
#         import Ecto.Schema
#         unquote(block)
#       after
#         :ok
#       end

#       primary_key_fields = @ecto_primary_keys |> Enum.reverse
#       autogenerate = @ecto_autogenerate |> Enum.reverse
#       autoupdate = @ecto_autoupdate |> Enum.reverse
#       fields = @ecto_fields |> Enum.reverse
#       assocs = @ecto_assocs |> Enum.reverse
#       embeds = @ecto_embeds |> Enum.reverse

#       Module.eval_quoted __ENV__, [
#         Ecto.Schema.__defstruct__(@struct_fields),
#         Ecto.Schema.__changeset__(@changeset_fields),
#         Ecto.Schema.__schema__(prefix, source, fields, primary_key_fields),
#         Ecto.Schema.__types__(fields),
#         Ecto.Schema.__assocs__(assocs),
#         Ecto.Schema.__embeds__(embeds),
#         Ecto.Schema.__read_after_writes__(@ecto_raw),
#         Ecto.Schema.__autogenerate__(@ecto_autogenerate_id, autogenerate, autoupdate)]
#     end
#   end

#   ## API
#   defmacro field(name, type \\ :string, opts \\ []) do
#     quote do
#       Ecto.Schema.__field__(__MODULE__, unquote(name), unquote(type), unquote(opts))
#     end
#   end

#   defmacro timestamps(opts \\ []) do
#     quote bind_quoted: binding() do
#       timestamps =
#         [inserted_at: :inserted_at, updated_at: :updated_at,
#          type: Ecto.DateTime, usec: false]
#         |> Keyword.merge(@timestamps_opts)
#         |> Keyword.merge(opts)

#       type    = Keyword.fetch!(timestamps, :type)
#       args    = if Keyword.fetch!(timestamps, :usec), do: [:usec], else: [:sec]
#       autogen = timestamps[:autogenerate] || {Ecto.DateTime, :autogenerate, args}

#       # TODO: Revisit and expose autogenerate when Elixir's 1.3
#       # calendar types are introduced .

#       if inserted_at = Keyword.fetch!(timestamps, :inserted_at) do
#         Ecto.Schema.field(inserted_at, type, [])
#         Module.put_attribute(__MODULE__, :ecto_autogenerate, {inserted_at, autogen})
#       end

#       if updated_at = Keyword.fetch!(timestamps, :updated_at) do
#         Ecto.Schema.field(updated_at, type, [])
#         Module.put_attribute(__MODULE__, :ecto_autogenerate, {updated_at, autogen})
#         Module.put_attribute(__MODULE__, :ecto_autoupdate, {updated_at, autogen})
#       end
#     end
#   end

#   defmacro has_many(name, queryable, opts \\ []) do
#     quote do
#       Ecto.Schema.__has_many__(__MODULE__, unquote(name), unquote(queryable), unquote(opts))
#     end
#   end

#   defmacro has_one(name, queryable, opts \\ []) do
#     quote do
#       Ecto.Schema.__has_one__(__MODULE__, unquote(name), unquote(queryable), unquote(opts))
#     end
#   end

#   defmacro belongs_to(name, queryable, opts \\ []) do
#     quote do
#       Ecto.Schema.__belongs_to__(__MODULE__, unquote(name), unquote(queryable), unquote(opts))
#     end
#   end

#   defmacro many_to_many(name, queryable, opts \\ []) do
#     quote do
#       Ecto.Schema.__many_to_many__(__MODULE__, unquote(name), unquote(queryable), unquote(opts))
#     end
#   end

#   defmacro embeds_one(name, schema, opts \\ []) do
#     quote do
#       Ecto.Schema.__embeds_one__(__MODULE__, unquote(name), unquote(schema), unquote(opts))
#     end
#   end

#   defmacro embeds_many(name, schema, opts \\ []) do
#     quote do
#       Ecto.Schema.__embeds_many__(__MODULE__, unquote(name), unquote(schema), unquote(opts))
#     end
#   end

#   ## Callbacks

#   @doc false
#   def __load__(schema, prefix, source, context, data, loader) do
#     struct = schema.__struct__()
#     fields = schema.__schema__(:types)

#     case do_load(struct, fields, data, loader) do
#       %{__meta__: %Metadata{} = metadata} = struct ->
#         source = source || schema.__schema__(:source)
#         metadata = %{metadata | state: :loaded, source: {prefix, source}, context: context}
#         Map.put(struct, :__meta__, metadata)
#       struct ->
#         struct
#     end
#   end

#   defp do_load(struct, types, map, loader) when is_map(map) do
#     Enum.reduce(types, struct, fn
#       {field, type}, acc ->
#         case Map.fetch(map, Atom.to_string(field)) do
#           {:ok, value} -> Map.put(acc, field, load!(struct, type, value, loader))
#           :error -> acc
#         end
#     end)
#   end

#   defp do_load(struct, types, {fields, values}, loader) when is_list(fields) and is_list(values) do
#     do_load(fields, values, struct, types, loader)
#   end

#   defp do_load([field|fields], [value|values], struct, types, loader) do
#     case Map.fetch(types, field) do
#       {:ok, type} ->
#         value = load!(struct, type, value, loader)
#         do_load(fields, values, Map.put(struct, field, value), types, loader)
#       :error ->
#         raise ArgumentError, "unknown field `#{field}` for struct #{inspect struct.__struct__}"
#     end
#   end

#   defp do_load([], [], struct, _types, _loader), do: struct

#   defp load!(struct, type, value, loader) do
#     case loader.(type, value) do
#       {:ok, value} -> value
#       :error -> raise ArgumentError, "cannot load `#{inspect value}` as type #{inspect type} in schema #{inspect struct.__struct__}"
#     end
#   end

#   @doc false
#   def __field__(mod, name, type, opts) do
#     check_type!(name, type, opts[:virtual])
#     pk? = opts[:primary_key] || false

#     default = default_for_type(type, opts)
#     Module.put_attribute(mod, :changeset_fields, {name, type})
#     put_struct_field(mod, name, default)

#     unless opts[:virtual] do
#       if raw = opts[:read_after_writes] do
#         Module.put_attribute(mod, :ecto_raw, name)
#       end

#       case gen = opts[:autogenerate] do
#         {_, _, _} ->
#           store_mfa_autogenerate!(mod, name, type, gen)
#         true ->
#           store_type_autogenerate!(mod, name, type, pk?)
#         _ ->
#           :ok
#       end

#       if raw && gen do
#         raise ArgumentError, "cannot mark the same field as autogenerate and read_after_writes"
#       end

#       if pk? do
#         Module.put_attribute(mod, :ecto_primary_keys, name)
#       end

#       Module.put_attribute(mod, :ecto_fields, {name, type})
#     end
#   end

#   @valid_has_options [:foreign_key, :references, :through, :on_delete, :defaults, :on_replace]

#   @doc false
#   def __has_many__(mod, name, queryable, opts) do
#     check_options!(opts, @valid_has_options, "has_many/3")

#     if is_list(queryable) and Keyword.has_key?(queryable, :through) do
#       association(mod, :many, name, Ecto.Association.HasThrough, queryable)
#     else
#       struct =
#         association(mod, :many, name, Ecto.Association.Has, [queryable: queryable] ++ opts)
#       Module.put_attribute(mod, :changeset_fields, {name, {:assoc, struct}})
#     end
#   end

#   @doc false
#   def __has_one__(mod, name, queryable, opts) do
#     check_options!(opts, @valid_has_options, "has_one/3")

#     if is_list(queryable) and Keyword.has_key?(queryable, :through) do
#       association(mod, :one, name, Ecto.Association.HasThrough, queryable)
#     else
#       struct =
#         association(mod, :one, name, Ecto.Association.Has, [queryable: queryable] ++ opts)
#       Module.put_attribute(mod, :changeset_fields, {name, {:assoc, struct}})
#     end
#   end
#   # :primary_key is valid here to support associative entity
#   # https://en.wikipedia.org/wiki/Associative_entity
#   @valid_belongs_to_options [:foreign_key, :references, :define_field, :type, :on_replace, :defaults, :primary_key]

#   @doc false
#   def __belongs_to__(mod, name, queryable, opts) do
#     check_options!(opts, @valid_belongs_to_options, "belongs_to/3")

#     opts = Keyword.put_new(opts, :foreign_key, :"#{name}_id")
#     foreign_key_type = opts[:type] || Module.get_attribute(mod, :foreign_key_type)

#     if name == Keyword.get(opts, :foreign_key) do
#       raise ArgumentError, "foreign_key #{inspect name} must be distinct from corresponding association name"
#     end

#     if Keyword.get(opts, :define_field, true) do
#       __field__(mod, opts[:foreign_key], foreign_key_type, opts)
#     end

#     struct =
#       association(mod, :one, name, Ecto.Association.BelongsTo, [queryable: queryable] ++ opts)
#     Module.put_attribute(mod, :changeset_fields, {name, {:assoc, struct}})
#   end

#   @valid_many_to_many_options [:join_through, :join_keys, :on_delete, :defaults, :on_replace]

#   @doc false
#   def __many_to_many__(mod, name, queryable, opts) do
#     check_options!(opts, @valid_many_to_many_options, "many_to_many/3")

#     struct =
#       association(mod, :many, name, Ecto.Association.ManyToMany, [queryable: queryable] ++ opts)
#     Module.put_attribute(mod, :changeset_fields, {name, {:assoc, struct}})
#   end

#   @doc false
#   def __embeds_one__(mod, name, schema, opts) do
#     check_options!(opts, [:strategy, :on_replace], "embeds_one/3")
#     embed(mod, :one, name, schema, opts)
#   end

#   @doc false
#   def __embeds_many__(mod, name, schema, opts) do
#     check_options!(opts, [:strategy, :on_replace], "embeds_many/3")
#     opts = Keyword.put(opts, :default, [])
#     embed(mod, :many, name, schema, opts)
#   end

#   ## Quoted callbacks

#   @doc false
#   def __changeset__(changeset_fields) do
#     map = changeset_fields |> Enum.into(%{}) |> Macro.escape()
#     quote do
#       def __changeset__, do: unquote(map)
#     end
#   end

#   @doc false
#   def __defstruct__(struct_fields) do
#     quote do
#       defstruct unquote(Macro.escape(struct_fields))
#     end
#   end

#   @doc false
#   def __schema__(prefix, source, fields, primary_key) do
#     field_names = Enum.map(fields, &elem(&1, 0))

#     # Hash is used by the query cache to specify
#     # the underlying schema structure did not change.
#     # We don't include the source because the source
#     # is already part of the query cache itself.
#     hash = :erlang.phash2({primary_key, fields})

#     quote do
#       def __schema__(:query),       do: %Ecto.Query{from: {unquote(source), __MODULE__}, prefix: unquote(prefix)}
#       def __schema__(:prefix),      do: unquote(prefix)
#       def __schema__(:source),      do: unquote(source)
#       def __schema__(:fields),      do: unquote(field_names)
#       def __schema__(:primary_key), do: unquote(primary_key)
#       def __schema__(:hash),        do: unquote(hash)
#     end
#   end

#   @doc false
#   def __types__(fields) do
#     quoted =
#       Enum.map(fields, fn {name, type} ->
#         quote do
#           def __schema__(:type, unquote(name)) do
#             unquote(Macro.escape(type))
#           end
#         end
#       end)

#     types = Macro.escape(Map.new(fields))

#     quote do
#       def __schema__(:types), do: unquote(types)
#       unquote(quoted)
#       def __schema__(:type, _), do: nil
#     end
#   end

#   @doc false
#   def __assocs__(assocs) do
#     quoted =
#       Enum.map(assocs, fn {name, refl} ->
#         quote do
#           def __schema__(:association, unquote(name)) do
#             unquote(Macro.escape(refl))
#           end
#         end
#       end)

#     assoc_names = Enum.map(assocs, &elem(&1, 0))

#     quote do
#       def __schema__(:associations), do: unquote(assoc_names)
#       unquote(quoted)
#       def __schema__(:association, _), do: nil
#     end
#   end

#   @doc false
#   def __embeds__(embeds) do
#     quoted =
#       Enum.map(embeds, fn {name, refl} ->
#         quote do
#           def __schema__(:embed, unquote(name)) do
#             unquote(Macro.escape(refl))
#           end
#         end
#       end)

#     embed_names = Enum.map(embeds, &elem(&1, 0))

#     quote do
#       def __schema__(:embeds), do: unquote(embed_names)
#       unquote(quoted)
#       def __schema__(:embed, _), do: nil
#     end
#   end

#   @doc false
#   def __read_after_writes__(fields) do
#     quote do
#       def __schema__(:read_after_writes), do: unquote(Enum.reverse(fields))
#     end
#   end

#   @doc false
#   def __autogenerate__(id, insert, update) do
#     quote do
#       def __schema__(:autogenerate_id), do: unquote(id)
#       def __schema__(:autogenerate), do: unquote(Macro.escape(insert))
#       def __schema__(:autoupdate), do: unquote(Macro.escape(update))
#     end
#   end

#   ## Private

#   defp association(mod, cardinality, name, association, opts) do
#     not_loaded  = %Ecto.Association.NotLoaded{__owner__: mod,
#                     __field__: name, __cardinality__: cardinality}
#     put_struct_field(mod, name, not_loaded)
#     opts = [cardinality: cardinality] ++ opts
#     struct = association.struct(mod, name, opts)
#     Module.put_attribute(mod, :ecto_assocs, {name, struct})

#     struct
#   end

#   defp embed(mod, cardinality, name, schema, opts) do
#     opts   = [cardinality: cardinality, related: schema] ++ opts
#     struct = Ecto.Embedded.struct(mod, name, opts)

#     __field__(mod, name, {:embed, struct}, opts)
#     Module.put_attribute(mod, :ecto_embeds, {name, struct})
#   end

#   defp put_struct_field(mod, name, assoc) do
#     fields = Module.get_attribute(mod, :struct_fields)

#     if List.keyfind(fields, name, 0) do
#       raise ArgumentError, "field/association #{inspect name} is already set on schema"
#     end

#     Module.put_attribute(mod, :struct_fields, {name, assoc})
#   end

#   defp check_options!(opts, valid, fun_arity) do
#     case Enum.find(opts, fn {k, _} -> not k in valid end) do
#       {k, _} ->
#         raise ArgumentError, "invalid option #{inspect k} for #{fun_arity}"
#       nil ->
#         :ok
#     end
#   end

#   defp check_type!(name, type, virtual?) do
#     cond do
#       type == :any and not virtual? ->
#         raise ArgumentError, "only virtual fields can have type :any, " <>
#                              "invalid type for field #{inspect name}"
#       Ecto.Type.primitive?(type) and not type in [:date, :time, :datetime] ->
#         true
#       is_atom(type) ->
#         if Code.ensure_compiled?(type) and function_exported?(type, :type, 0) do
#           type
#         else
#           raise_type_error(name, type)
#         end
#       true ->
#         raise ArgumentError, "invalid type #{inspect type} for field #{inspect name}"
#     end
#   end

#   defp raise_type_error(name, type) do
#     raise ArgumentError, "invalid or unknown type #{inspect type} for field #{inspect name}" <>
#                          raise_type_error_hint(type)
#   end

#   defp raise_type_error_hint(:datetime),
#     do: ". Maybe you meant to use Ecto.DateTime?"
#   defp raise_type_error_hint(:date),
#     do: ". Maybe you meant to use Ecto.Date?"
#   defp raise_type_error_hint(:time),
#     do: ". Maybe you meant to use Ecto.Time?"
#   defp raise_type_error_hint(:uuid),
#     do: ". Maybe you meant to use Ecto.UUID?"
#   defp raise_type_error_hint(_),
#     do: ""

#   defp store_mfa_autogenerate!(mod, name, type, mfa) do
#     cond do
#       autogenerate_id(type) ->
#         raise ArgumentError, ":autogenerate with {m, f, a} not supported by ID types"
#       true ->
#         Module.put_attribute(mod, :ecto_autogenerate, {name, mfa})
#     end
#   end

#   defp store_type_autogenerate!(mod, name, type, pk?) do
#     cond do
#       id = autogenerate_id(type) ->
#         cond do
#           not pk? ->
#             raise ArgumentError, "only primary keys allow :autogenerate for type #{inspect type}, " <>
#                                  "field #{inspect name} is not a primary key"
#           Module.get_attribute(mod, :ecto_autogenerate_id) ->
#             raise ArgumentError, "only one primary key with ID type may be marked as autogenerated"
#           true ->
#             Module.put_attribute(mod, :ecto_autogenerate_id, {name, id})
#         end

#       Ecto.Type.primitive?(type) ->
#         raise ArgumentError, "field #{inspect name} does not support :autogenerate because it uses a " <>
#                              "primitive type #{inspect type}"

#       # Note the custom type has already been loaded in check_type!/3
#       not function_exported?(type, :autogenerate, 0) ->
#         raise ArgumentError, "field #{inspect name} does not support :autogenerate because it uses a " <>
#                              "custom type #{inspect type} that does not define autogenerate/0"

#       true ->
#         Module.put_attribute(mod, :ecto_autogenerate, {name, {type, :autogenerate, []}})
#     end
#   end

#   defp autogenerate_id(type) do
#     id = if Ecto.Type.primitive?(type), do: type, else: type.type
#     if id in [:id, :binary_id], do: id, else: nil
#   end

#   defp default_for_type(_, opts) do
#     Keyword.get(opts, :default)
#   end
# end
