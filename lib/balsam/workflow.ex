defmodule Balsam.Workflow do
  @moduledoc """
  Behavior for workflows (both single jobs and multi-step DAGs).
  Every executable unit in Balsam is a workflow.
  """

  @callback workflow_config() :: map()
  @callback run(progress_callback :: function() | nil) :: {:ok, any()} | {:error, any()}
  @callback validate_config(config :: map()) :: :ok | {:error, String.t()}

  # Make run/1 optional since multi-step workflows don't implement it directly
  @optional_callbacks [validate_config: 1, run: 1]

  def default_config do
    %{
      name: "Unnamed Workflow",
      description: "",
      schedule: :manual,
      max_concurrent_nodes: 1,
      max_retries: 3,
      timeout: :infinity,
      enable_progress: false
    }
  end

  def merge_config(workflow_config) when is_map(workflow_config) do
    Map.merge(default_config(), workflow_config)
  end
  def merge_config(_), do: default_config()

  @doc """
  Create a single-node workflow config from a module that implements run/1.
  """
  def single_node_config(module, custom_config \\ %{}) do
    workflow_id = generate_workflow_id(module)

    base_config = %{
      name: custom_config[:name] || "#{module}",
      description: custom_config[:description] || "Single node workflow",
      nodes: %{
        main: %{
          module: module,
          function: :run,
          args: [],
          depends_on: []
        }
      }
    }

    config = Map.merge(base_config, custom_config)
    {workflow_id, config}
  end

  defp generate_workflow_id(module_name) do
    module_name
    |> Atom.to_string()
    |> String.replace("Elixir.", "")
    |> String.replace("ETL.", "")
    |> String.replace("Pipeline.", "")
    |> String.replace(~r/([a-z])([A-Z])/, "\\1_\\2")
    |> String.downcase()
    |> String.replace(".", "_")
    |> String.replace(~r/[^a-z0-9_]/, "_")
    |> String.replace(~r/_+/, "_")
    |> String.trim("_")
    |> String.to_atom()
  end
end
