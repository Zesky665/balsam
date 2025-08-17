# Generate one migration:
# mix ecto.gen.migration create_balsam_tables
defmodule Balsam.Repo.Migrations.CreateBalsamTables do
  use Ecto.Migration

  def change do
    # Schema Migrations - Ecto's internal table for tracking migrations
    create table(:schema_migrations, primary_key: false) do
      add :version, :bigint, primary_key: true
      add :inserted_at, :naive_datetime
    end
    # Job Definitions - stores registered job configurations
    create table(:job_definitions) do
      add :job_id, :string, null: false
      add :module, :string, null: false
      add :function, :string, null: false
      add :args, :string  # JSON string
      add :schedule, :string  # JSON string
      add :enable_progress, :boolean, default: false
      add :max_retries, :integer, default: 3
      add :timeout, :string, default: "infinity"
      add :active, :boolean, default: true
      add :last_run, :integer  # timestamp in milliseconds

      timestamps(type: :utc_datetime_usec)
    end

    # Job Runs - tracks individual job executions
    create table(:job_runs) do
      add :job_id, :string, null: false
      add :status, :string, null: false  # running, completed, failed, cancelled
      add :started_at, :utc_datetime_usec, null: false
      add :completed_at, :utc_datetime_usec
      add :duration_ms, :integer
      add :result, :map
      add :error_message, :string
      add :progress_data, :map
      add :metadata, :string  # JSON string

      timestamps(type: :utc_datetime_usec)
    end

    # Job Logs - stores log messages from job executions
    create table(:job_logs) do
      add :job_id, :string, null: false
      add :job_run_id, :integer
      add :level, :string, null: false  # debug, info, warning, error
      add :message, :string, null: false
      add :context, :string  # JSON string
      add :logged_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    # Job Progress - tracks progress updates during job execution
    create table(:job_progress) do
      add :job_id, :string, null: false
      add :job_run_id, :integer
      add :current, :integer, null: false
      add :total, :integer
      add :percentage, :float
      add :message, :string
      add :recorded_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    # DAG Definitions - stores multi-step workflow configurations
    create table(:dag_definitions) do
      add :dag_id, :string, null: false
      add :name, :string, null: false
      add :description, :string
      add :schedule, :string  # JSON string for schedule config
      add :max_concurrent_nodes, :integer, default: 3
      add :max_retries, :integer, default: 3
      add :timeout, :string, default: "infinity"
      add :active, :boolean, default: true
      add :last_run, :integer  # timestamp in milliseconds

      timestamps(type: :utc_datetime_usec)
    end

    # DAG Nodes - individual steps within a DAG
    create table(:dag_nodes) do
      add :node_id, :string, null: false
      add :dag_id, :string, null: false
      add :job_id, :string, null: false  # References existing JobDefinition
      add :depends_on, {:array, :string}  # List of node_ids this depends on
      add :condition_module, :string  # Optional: module for conditional execution
      add :condition_function, :string  # Optional: function for conditional execution
      add :node_config, :string  # JSON string for node-specific config overrides

      timestamps(type: :utc_datetime_usec)
    end

    # DAG Runs - tracks individual DAG executions
    create table(:dag_runs) do
      add :dag_id, :string, null: false
      add :run_id, :string, null: false
      add :status, :string, null: false  # running, completed, failed, cancelled
      add :started_at, :utc_datetime_usec, null: false
      add :completed_at, :utc_datetime_usec
      add :duration_ms, :integer
      add :node_statuses, :map  # Map of node_id -> status
      add :node_results, :map  # Map of node_id -> result
      add :metadata, :string  # JSON string for run metadata

      timestamps(type: :utc_datetime_usec)
    end

    # Indexes for performance
    create unique_index(:job_definitions, [:job_id])

    create index(:job_runs, [:job_id])
    create index(:job_runs, [:status])
    create index(:job_runs, [:started_at])

    create index(:job_logs, [:job_id])
    create index(:job_logs, [:job_run_id])
    create index(:job_logs, [:level])
    create index(:job_logs, [:logged_at])

    create index(:job_progress, [:job_id])
    create index(:job_progress, [:job_run_id])
    create index(:job_progress, [:recorded_at])

    create unique_index(:dag_definitions, [:dag_id])

    create unique_index(:dag_nodes, [:dag_id, :node_id])
    create index(:dag_nodes, [:dag_id])

    create unique_index(:dag_runs, [:run_id])
    create index(:dag_runs, [:dag_id])
    create index(:dag_runs, [:status])
    create index(:dag_runs, [:started_at])
  end
end
