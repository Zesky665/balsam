# test/support/test_setup.exs
defmodule TestSetup do
  @moduledoc """
  Helper module for setting up test environment
  """

  def ensure_database_migrated do
    try do
      # Try to run the migration
      case Ecto.Migrator.up(Balsam.Repo, 0, Balsam.Repo.Migrations.CreateBalsamTables, log: false) do
        :ok -> :ok
        :already_up -> :ok
        {:error, _} -> :ok  # Migration might already exist
      end
    rescue
      _ ->
        # If migration fails, try to create tables manually for tests
        create_test_tables()
    end
  end

  defp create_test_tables do
    try do
      # Create minimal tables needed for tests
      Ecto.Adapters.SQL.query!(Balsam.Repo, """
        CREATE TABLE IF NOT EXISTS job_runs (
          id INTEGER PRIMARY KEY,
          job_id TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          completed_at TEXT,
          duration_ms INTEGER,
          result TEXT,
          error_message TEXT,
          progress_data TEXT,
          metadata TEXT,
          inserted_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      """)

      Ecto.Adapters.SQL.query!(Balsam.Repo, """
        CREATE TABLE IF NOT EXISTS job_logs (
          id INTEGER PRIMARY KEY,
          job_id TEXT NOT NULL,
          job_run_id INTEGER,
          level TEXT NOT NULL,
          message TEXT NOT NULL,
          context TEXT,
          logged_at TEXT NOT NULL,
          inserted_at TEXT NOT NULL
        )
      """)

      Ecto.Adapters.SQL.query!(Balsam.Repo, """
        CREATE TABLE IF NOT EXISTS job_progress (
          id INTEGER PRIMARY KEY,
          job_id TEXT NOT NULL,
          job_run_id INTEGER,
          current INTEGER NOT NULL,
          total INTEGER,
          percentage REAL,
          message TEXT,
          recorded_at TEXT NOT NULL,
          inserted_at TEXT NOT NULL
        )
      """)

      Ecto.Adapters.SQL.query!(Balsam.Repo, """
        CREATE TABLE IF NOT EXISTS dag_runs (
          id INTEGER PRIMARY KEY,
          dag_id TEXT NOT NULL,
          run_id TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          completed_at TEXT,
          duration_ms INTEGER,
          node_statuses TEXT,
          node_results TEXT,
          metadata TEXT,
          inserted_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
      """)

      :ok
    rescue
      _ -> :ok  # Ignore if table creation fails
    end
  end

  def clean_test_database do
    tables = ["job_runs", "job_logs", "job_progress", "dag_runs", "job_definitions"]

    Enum.each(tables, fn table ->
      try do
        Ecto.Adapters.SQL.query!(Balsam.Repo, "DELETE FROM #{table}")
      rescue
        _ -> :ok  # Ignore if table doesn't exist
      end
    end)
  end
end
