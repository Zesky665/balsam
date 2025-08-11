defmodule Balsam.Repo do
  use Ecto.Repo,
    otp_app: :balsam,
    adapter: Ecto.Adapters.SQLite3
end
