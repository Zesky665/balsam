defmodule Balsam.Repo.Migrations.CreateDagRuns do
  use Ecto.Migration

  def change do
    create table(:dag_runs) do
      add :dag_id, :string, null: false
      add :run_id, :string, null: false
      add :status, :string, null: false  # running, completed, failed, cancelled
      add :started_at, :utc_datetime_usec, null: false
      add :completed_at, :utc_datetime_usec
      add :duration_ms, :integer
      add :node_statuses, :map, default: %{}  # Map of node_id -> status
      add :node_results, :map, default: %{}   # Map of node_id -> result
      add :metadata, :text  # JSON string for run metadata

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:dag_runs, [:run_id])
    create index(:dag_runs, [:dag_id])
    create index(:dag_runs, [:status])
    create index(:dag_runs, [:started_at])
  end
end
