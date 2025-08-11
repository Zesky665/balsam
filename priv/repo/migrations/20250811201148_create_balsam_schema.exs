defmodule Balsam.Repo.Migrations.CreateBalsamSchema do
  use Ecto.Migration

  def change do
    # Job definitions - persistent job storage
    create table(:job_definitions) do
      add :job_id, :string, null: false
      add :module, :string, null: false
      add :function, :string, null: false
      add :args, :text
      add :schedule, :text
      add :enable_progress, :boolean, default: false
      add :max_retries, :integer, default: 3
      add :timeout, :string, default: "infinity"
      add :active, :boolean, default: true
      add :last_run, :bigint

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:job_definitions, [:job_id])
    create index(:job_definitions, [:active])

    # Job runs - execution history
    create table(:job_runs) do
      add :job_id, :string, null: false
      add :status, :string, null: false
      add :started_at, :utc_datetime_usec, null: false
      add :completed_at, :utc_datetime_usec
      add :duration_ms, :integer
      add :result, :text
      add :error_message, :text
      add :progress_data, :text
      add :metadata, :text

      timestamps(type: :utc_datetime_usec)
    end

    create index(:job_runs, [:job_id])
    create index(:job_runs, [:status])
    create index(:job_runs, [:started_at])

    # Job logs - detailed logging
    create table(:job_logs) do
      add :job_id, :string, null: false
      add :job_run_id, references(:job_runs, on_delete: :delete_all)
      add :level, :string, null: false
      add :message, :text, null: false
      add :context, :text
      add :logged_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create index(:job_logs, [:job_id])
    create index(:job_logs, [:job_run_id])
    create index(:job_logs, [:level])
    create index(:job_logs, [:logged_at])

    # Job progress - progress tracking
    create table(:job_progress) do
      add :job_id, :string, null: false
      add :job_run_id, references(:job_runs, on_delete: :delete_all)
      add :current, :integer, null: false
      add :total, :integer
      add :percentage, :float
      add :message, :string
      add :recorded_at, :utc_datetime_usec, null: false

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create index(:job_progress, [:job_id])
    create index(:job_progress, [:job_run_id])
    create index(:job_progress, [:recorded_at])
  end
end
