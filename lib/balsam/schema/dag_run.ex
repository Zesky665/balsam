defmodule Balsam.Schema.DagRun do
  use Ecto.Schema
  import Ecto.Changeset

  @moduledoc """
  Tracks individual DAG execution instances.
  """

  schema "dag_runs" do
    field :dag_id, :string
    field :run_id, :string
    field :status, Ecto.Enum, values: [:running, :completed, :failed, :cancelled]
    field :started_at, :utc_datetime_usec
    field :completed_at, :utc_datetime_usec
    field :duration_ms, :integer
    field :node_statuses, :map  # Map of node_id -> status
    field :node_results, :map  # Map of node_id -> result
    field :metadata, :string  # JSON string for run metadata

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(dag_run, attrs) do
    dag_run
    |> cast(attrs, [:dag_id, :run_id, :status, :started_at, :completed_at,
                    :duration_ms, :node_statuses, :node_results, :metadata])
    |> validate_required([:dag_id, :run_id, :status, :started_at])
    |> validate_inclusion(:status, [:running, :completed, :failed, :cancelled])
    |> unique_constraint(:run_id)
  end

  def start_changeset(dag_run, attrs) do
    dag_run
    |> cast(attrs, [:dag_id, :run_id, :started_at, :metadata])
    |> validate_required([:dag_id, :run_id, :started_at])
    |> put_change(:status, :running)
    |> put_change(:node_statuses, %{})
    |> put_change(:node_results, %{})
  end

  def complete_changeset(dag_run, attrs) do
    duration = if dag_run.started_at && attrs[:completed_at] do
      DateTime.diff(attrs[:completed_at], dag_run.started_at, :millisecond)
    else
      nil
    end

    dag_run
    |> cast(attrs, [:status, :completed_at, :node_statuses, :node_results])
    |> validate_required([:status, :completed_at])
    |> put_change(:duration_ms, duration)
  end
end
