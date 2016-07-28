ExUnit.start

defmodule EctoRiak.TestRepo do
  use Ecto.Repo, otp_app: :ecto
end

defmodule EctoRiak.Case do
  use ExUnit.CaseTemplate

  setup do
    {:ok, pid } = EctoRiak.TestRepo.start_link()

    on_exit fn ->
      Process.exit(pid, :kill)
    end

    {:ok, pid: pid}
  end
end
