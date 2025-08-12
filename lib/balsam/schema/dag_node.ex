defmodule Balsam.Schema.DagNode do
  use Ecto.Schema
  import Ecto.Changeset

  @moduledoc """
  Individual nodes within a DAG.
  Each node references an existing job definition and specifies dependencies.
  """

  schema "dag_nodes" do
    field :node_id, :string
    field :dag_id, :string
    field :job_id, :string  # References existing JobDefinition
    field :depends_on, {:array, :string}  # List of node_ids this depends on
    field :condition_module, :string  # Optional: module for conditional execution
    field :condition_function, :string  # Optional: function for conditional execution
    field :node_config, :string  # JSON string for node-specific config overrides

    timestamps(type: :utc_datetime_usec)
  end

  def changeset(node, attrs) do
    node
    |> cast(attrs, [:node_id, :dag_id, :job_id, :depends_on, :condition_module,
                    :condition_function, :node_config])
    |> validate_required([:node_id, :dag_id, :job_id])
    |> unique_constraint([:dag_id, :node_id])
  end
end
