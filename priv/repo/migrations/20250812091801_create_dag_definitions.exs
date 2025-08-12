defmodule Balsam.Repo.Migrations.CreateDagDefinitions do
  use Ecto.Migration

  def change do
    create table(:dag_definitions) do
      add :dag_id, :string, null: false
      add :name, :string, null: false
      add :description, :text
      add :schedule, :text  # JSON string
      add :max_concurrent_nodes, :integer, default: 3
      add :max_retries, :integer, default: 3
      add :timeout, :string, default: "infinity"
      add :active, :boolean, default: true
      add :last_run, :bigint  # timestamp in milliseconds

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:dag_definitions, [:dag_id])
    create index(:dag_definitions, [:active])
  end
end
