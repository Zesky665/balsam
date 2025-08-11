defmodule Balsam.Schema.JobLog do
  use Ecto.Schema
  import Ecto.Changeset

  schema "job_logs" do
    field :job_id, :string
    field :job_run_id, :integer
    field :level, Ecto.Enum, values: [:debug, :info, :warning, :error]
    field :message, :string
    field :context, :string  # Change to string to store JSON
    field :logged_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  def changeset(log, attrs) do
    log
    |> cast(attrs, [:job_id, :job_run_id, :level, :message, :context, :logged_at])
    |> validate_required([:job_id, :level, :message, :logged_at])
    |> validate_inclusion(:level, [:debug, :info, :warning, :error])
  end
end
