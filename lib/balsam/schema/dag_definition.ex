defmodule Balsam.Schema.DagDefinition do
  use Ecto.Schema
  import Ecto.Changeset

  @moduledoc """
  Persistent storage for DAG definitions.
  DAGs are composed of multiple job nodes with dependencies.
  """

  schema "dag_definitions" do
    field :dag_id, :string
    field :name, :string
    field :description, :string
    field :schedule, :string  # JSON string for schedule config
    field :max_concurrent_nodes, :integer, default: 3
    field :max_retries, :integer, default: 3
    field :timeout, :string, default: "infinity"
    field :active, :boolean, default: true
    field :last_run, :integer  # timestamp in milliseconds

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(dag_def, attrs) do
    dag_def
    |> cast(attrs, [:dag_id, :name, :description, :schedule, :max_concurrent_nodes,
                    :max_retries, :timeout, :active, :last_run])
    |> validate_required([:dag_id, :name])
    |> unique_constraint(:dag_id)
    |> validate_number(:max_concurrent_nodes, greater_than: 0)
  end

  def from_config(dag_id, config) do
    %{
      dag_id: to_string(dag_id),
      name: config[:name] || to_string(dag_id),
      description: config[:description] || "",
      schedule: inspect(config[:schedule] || :manual),
      max_concurrent_nodes: config[:max_concurrent_nodes] || 3,
      max_retries: config[:max_retries] || 3,
      timeout: to_string(config[:timeout] || :infinity),
      active: true,
      last_run: config[:last_run]
    }
  end

  def to_config(dag_def) do
    try do
      {schedule, _} = Code.eval_string(dag_def.schedule)

      timeout = case dag_def.timeout do
        "infinity" -> :infinity
        other -> String.to_integer(other)
      end

      config = %{
        name: dag_def.name,
        description: dag_def.description,
        schedule: schedule,
        max_concurrent_nodes: dag_def.max_concurrent_nodes,
        max_retries: dag_def.max_retries,
        timeout: timeout,
        last_run: dag_def.last_run
      }

      {:ok, config}
    rescue
      error ->
        {:error, error}
    end
  end
end
