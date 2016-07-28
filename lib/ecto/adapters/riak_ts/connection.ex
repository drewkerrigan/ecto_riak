defmodule Ecto.Adapters.RiakTS.Connection do

  def child_spec(options) do
    # All connections handled through pooler
    pool = Keyword.get(options, :pool, nil)
    create_pools(pool, options, 1)
    Supervisor.Spec.worker(Task, [fn -> :timer.sleep(:infinity) end])
  end

  defp create_pools([], _, _) do
    :ok
  end
  defp create_pools([pool | rest], options, count) do
    name = String.to_atom("riak" <> Integer.to_string(count))
    create_pool(name, pool) |> :pooler.new_pool
    create_pools(rest, options, count + 1)
  end
  defp create_pools(nil, options, _) do
    create_pool(:riak1, options) |> :pooler.new_pool
  end

  defp create_pool(name, options) do
    name = Keyword.get(options, :name, name)
    group = :riak
    host = convert_host(Keyword.get(options, :hostname, 'localhost'))
    port = Keyword.get(options, :port, 8087)
    init = Keyword.get(options, :init_count, 5)
    max = Keyword.get(options, :max_count, 10)
    start_mfa = Keyword.get(options, :start_mfa,
      {Riak.Connection, :start_link, [host, port]})
    [name: name,
     group: group,
     max_count: max,
     init_count: init,
     start_mfa: start_mfa]
  end

  defp convert_host(host) when is_binary(host) do
    :erlang.binary_to_list(host)
  end
  defp convert_host(host) when is_list(host) do
    host
  end
end
