defmodule Balsam.Schema.JobProgress do
  use Ecto.Schema
  import Ecto.Changeset

  schema "job_progress" do
    field :job_id, :string
    field :job_run_id, :integer
    field :current, :integer
    field :total, :integer
    field :percentage, :float
    field :message, :string
    field :recorded_at, :utc_datetime_usec

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end

  def changeset(progress, attrs) do
    percentage = calculate_percentage(attrs[:current], attrs[:total])

    progress
    |> cast(attrs, [:job_id, :job_run_id, :current, :total, :message, :recorded_at])
    |> validate_required([:job_id, :current, :recorded_at])
    |> put_change(:percentage, percentage)
  end

  defp calculate_percentage(current, total) when is_integer(current) and is_integer(total) and total > 0 do
    (current / total * 100) |> Float.round(2)
  end
  defp calculate_percentage(_, _), do: nil
end
