defmodule Ecto.Adapters.RiakTS.Storage do

  @behaviour Ecto.Adapter.Storage

  def storage_down(_options) do
    :ok
  end

  def storage_up(_options) do
    :ok
  end

end
