ExUnit.start

defmodule EctoRiak.RiakTSRepo do
  use Ecto.Repo, otp_app: :ecto
end
defmodule EctoRiak.RiakKVRepo do
  use Ecto.Repo, otp_app: :ecto
end

defmodule EctoRiak.Case do
  use ExUnit.CaseTemplate

  setup do
    {:ok, ts_pid } = EctoRiak.RiakTSRepo.start_link()
    {:ok, kv_pid } = EctoRiak.RiakKVRepo.start_link()

    on_exit fn ->
      Process.exit(ts_pid, :kill)
      Process.exit(kv_pid, :kill)
    end

    {:ok, ts_pid: ts_pid, kv_pid: kv_pid}
  end
end
