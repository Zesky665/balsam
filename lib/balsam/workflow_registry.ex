defmodule Balsam.WorkflowRegistry do
  @moduledoc """
  Discovers and registers all workflows in the system.
  """

  require Logger

  def register_all do
    try do
      ensure_modules_loaded()
      :timer.sleep(200)

      workflows = discover_workflows()

      if length(workflows) == 0 do
        Logger.warning("No workflows discovered. Implement @behaviour Balsam.Workflow")
        Logger.info("Available modules: #{inspect(get_workflow_modules())}")
      end

      Enum.each(workflows, fn {workflow_id, workflow_config} ->
        case Balsam.Orchestrator.register_dag(workflow_id, workflow_config) do
          :ok ->
            node_count = map_size(workflow_config[:nodes] || %{})
            type = if node_count == 1, do: "job", else: "DAG (#{node_count} nodes)"
            Logger.info("Registered #{type}: #{workflow_id}")
          {:error, reason} ->
            Logger.error("Failed to register workflow #{workflow_id}: #{inspect(reason)}")
        end
      end)

      Logger.info("Workflow registration complete: #{length(workflows)} workflows")
    rescue
      error ->
        Logger.error("Failed to register workflows: #{inspect(error)}")
    end
  end

  @doc false
  def discover_workflows do
    :code.all_loaded()
    |> Enum.filter(&workflow_module?/1)
    |> Enum.map(&extract_workflow_config/1)
    |> Enum.filter(&(&1 != nil))
    |> Enum.sort_by(fn {workflow_id, _config} -> workflow_id end)
  end

  defp workflow_module?({module_name, _loaded_from}) do
    try do
      implements_workflow_behavior?(module_name) and
      not is_test_module?(module_name)
    rescue
      _ -> false
    end
  end

  defp implements_workflow_behavior?(module_name) do
    try do
      case module_name.module_info(:attributes) do
        attributes when is_list(attributes) ->
          behaviors = Keyword.get_values(attributes, :behaviour) ++
                     Keyword.get_values(attributes, :behavior)
          behaviors = List.flatten(behaviors)
          Balsam.Workflow in behaviors
        _ -> false
      end
    rescue
      _ -> false
    end
  end

  defp extract_workflow_config({module_name, _}) do
    try do
      workflow_id = generate_workflow_id(module_name)

      workflow_config = try do
        apply(module_name, :workflow_config, [])
        |> Balsam.Workflow.merge_config()
      rescue
        _ ->
          Logger.warning("#{module_name} implements Balsam.Workflow but workflow_config/0 failed")
          nil
      end

      if workflow_config do
        {workflow_id, workflow_config}
      else
        nil
      end
    rescue
      error ->
        Logger.warning("Failed to extract workflow config for #{module_name}: #{inspect(error)}")
        nil
    end
  end

  defp generate_workflow_id(module_name) do
    module_name
    |> Atom.to_string()
    |> String.replace("Elixir.", "")
    |> String.replace("ETL.", "")
    |> String.replace("Pipeline.", "")
    |> String.replace("DAG.", "")
    |> String.replace(~r/([a-z])([A-Z])/, "\\1_\\2")
    |> String.downcase()
    |> String.replace(".", "_")
    |> String.replace(~r/[^a-z0-9_]/, "_")
    |> String.replace(~r/_+/, "_")
    |> String.trim("_")
    |> String.to_atom()
  end

  defp is_test_module?(module_name) do
    module_string = Atom.to_string(module_name)
    String.contains?(module_string, "Test") or
    String.contains?(module_string, "Support") or
    String.contains?(module_string, "ExUnit")
  end

  defp ensure_modules_loaded do
    try do
      Code.compile_file("etl/**/*.ex")
    rescue
      _ -> :ok
    end
  end

  defp get_workflow_modules do
    :code.all_loaded()
    |> Enum.filter(fn {module_name, _} ->
      module_string = Atom.to_string(module_name)
      String.contains?(module_string, "ETL") or
      String.contains?(module_string, "Pipeline") or
      String.contains?(module_string, "Workflow")
    end)
    |> Enum.map(fn {module_name, _} -> module_name end)
  end

  def list_workflows do
    discover_workflows()
    |> Enum.map(fn {workflow_id, config} ->
      node_count = map_size(config[:nodes] || %{})
      %{
        workflow_id: workflow_id,
        name: config[:name],
        type: if(node_count == 1, do: :job, else: :dag),
        node_count: node_count,
        schedule: config[:schedule]
      }
    end)
  end
end
