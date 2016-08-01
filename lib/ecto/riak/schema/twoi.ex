defmodule Ecto.Riak.Schema.TwoI do
  defmacro index(name, type \\ :string, opts \\ []) do
    quote do
      Ecto.Riak.Schema.TwoI.__index__(__MODULE__, unquote(name), unquote(type), unquote(opts))
    end
  end

  def __index__(mod, name, type, _opts) do
    Module.put_attribute(mod, :ecto_riak_indexes, {name, type})
  end

  defmacro __using__(_) do
    quote do
      Module.register_attribute(__MODULE__, :ecto_riak_indexes, accumulate: true)

      use Ecto.Schema
      import Ecto.Riak.Schema.TwoI, only: [index: 3]

      @secondary_indexes nil

      def secondary_indexes() do
        Module.get_attribute(mod, :ecto_riak_indexes)
        # case @secondary_indexes do
        #   false ->
        #     []
        #   indexes ->
        #     indexes
        # end
      end
    end
  end
end
