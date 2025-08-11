defmodule Balsam.Schema.JobRun do
  use Ecto.Schema
  import Ecto.Changeset

  schema "job_runs" do
    field :job_id, :string
    field :status, Ecto.Enum, values: [:running, :completed, :failed, :cancelled]
    field :started_at, :utc_datetime_usec
    field :completed_at, :utc_datetime_usec
    field :duration_ms, :integer
    field :result, :map
    field :error_message, :string
    field :progress_data, :map
    field :metadata, :string  # Change to string to store JSON

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(job_run, attrs) do
    job_run
    |> cast(attrs, [:job_id, :status, :started_at, :completed_at, :duration_ms,
                    :result, :error_message, :progress_data, :metadata])
    |> validate_required([:job_id, :status, :started_at])
    |> validate_inclusion(:status, [:running, :completed, :failed, :cancelled])
  end

  def start_changeset(job_run, attrs) do
    job_run
    |> cast(attrs, [:job_id, :started_at, :metadata])
    |> validate_required([:job_id, :started_at])
    |> put_change(:status, :running)
  end

  def complete_changeset(job_run, attrs) do
    duration = if job_run.started_at && attrs[:completed_at] do
      DateTime.diff(attrs[:completed_at], job_run.started_at, :millisecond)
    else
      nil
    end

    job_run
    |> cast(attrs, [:status, :completed_at, :result, :error_message, :progress_data])
    |> validate_required([:status, :completed_at])
    |> put_change(:duration_ms, duration)
  end
end
