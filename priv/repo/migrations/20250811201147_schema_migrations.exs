# priv/repo/migrations/20250811201147_create_schema_migrations.exs
# NOTE: This timestamp (20250811201147) is ONE SECOND BEFORE your main migration

defmodule Balsam.Repo.Migrations.CreateSchemaMigrations do
  use Ecto.Migration

  def up do
    # Create the schema_migrations table that Ecto needs
    execute """
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version INTEGER PRIMARY KEY,
      inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """
  end

  def down do
    # Don't drop schema_migrations - it's needed by Ecto
    # execute "DROP TABLE IF EXISTS schema_migrations"
  end
end
