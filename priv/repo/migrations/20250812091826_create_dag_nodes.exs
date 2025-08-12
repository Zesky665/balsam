defmodule Balsam.Repo.Migrations.CreateDagNodes do
  use Ecto.Migration

  def change do
    create table(:dag_nodes) do
      add :node_id, :string, null: false
      add :dag_id, :string, null: false
      add :job_id, :string, null: false
      add :depends_on, {:array, :string}, default: []
      add :condition_module, :string
      add :condition_function, :string
      add :node_config, :text  # JSON string for node-specific config

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:dag_nodes, [:dag_id, :node_id])
    create index(:dag_nodes, [:dag_id])
  end
end
