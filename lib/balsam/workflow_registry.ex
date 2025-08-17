defmodule Balsam.WorkflowRegistry do
  @moduledoc """
  Enhanced workflow registry that discovers and registers all workflows in the system.
  Integrates with both the orchestrator and job storage for complete workflow management.
  """

  require Logger
  alias Balsam.{Orchestrator, Repo, Schema.JobDefinition}

  def register_all do
    try do
      ensure_modules_loaded()
      :timer.sleep(200)

      workflows = discover_workflows()

      if length(workflows) == 0 do
        Logger.warning("No workflows discovered. Implement @behaviour Balsam.Workflow")
        Logger.info("Available modules: #{inspect(get_workflow_modules())}")
      end

      # Register with both the orchestrator and job storage
      Enum.each(workflows, fn {workflow_id, workflow_config} ->
        case register_workflow_complete(workflow_id, workflow_config) do
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

  @doc """
  Discover all workflow modules that implement @behaviour Balsam.Workflow
  """
  def discover_workflows do
    :code.all_loaded()
    |> Enum.filter(&workflow_module?/1)
    |> Enum.map(&extract_workflow_config/1)
    |> Enum.filter(&(&1 != nil))
    |> Enum.sort_by(fn {workflow_id, _config} -> workflow_id end)
  end

  @doc """
  Validate a workflow configuration
  """
  def validate_workflow_config(_workflow_id, workflow_config) do
    with :ok <- validate_required_fields(workflow_config),
         :ok <- validate_nodes_structure(workflow_config.nodes),
         :ok <- validate_workflow_module(workflow_config) do
      :ok
    else
      error -> error
    end
  end

  @doc """
  Test a workflow without actually running it
  """
  def test_workflow(workflow_id) do
    case discover_workflows() |> Enum.find(fn {id, _} -> id == workflow_id end) do
      nil ->
        {:error, :workflow_not_found}

      {^workflow_id, workflow_config} ->
        case validate_workflow_config(workflow_id, workflow_config) do
          :ok ->
            try_dry_run(workflow_id, workflow_config)
          error ->
            error
        end
    end
  end

  @doc """
  List all discovered workflows with their metadata
  """
  def list_workflows do
    discover_workflows()
    |> Enum.map(fn {workflow_id, config} ->
      node_count = map_size(config[:nodes] || %{})
      %{
        workflow_id: workflow_id,
        name: config[:name],
        type: if(node_count == 1, do: :job, else: :dag),
        node_count: node_count,
        schedule: config[:schedule],
        description: config[:description]
      }
    end)
  end

  @doc """
  Get detailed information about a specific workflow
  """
  def get_workflow_info(workflow_id) do
    case discover_workflows() |> Enum.find(fn {id, _} -> id == workflow_id end) do
      nil -> {:error, :workflow_not_found}
      {^workflow_id, config} -> {:ok, config}
    end
  end

  ## Private Functions

  defp register_workflow_complete(workflow_id, workflow_config) do
    # First validate the configuration
    case validate_workflow_config(workflow_id, workflow_config) do
      :ok ->
        # Register with orchestrator
        case Orchestrator.register_workflow(workflow_id, workflow_config) do
          :ok ->
            # Also register as job definition for single-node workflows
            if is_single_node_workflow?(workflow_config) do
              register_as_job_definition(workflow_id, workflow_config)
            end
            :ok

          error ->
            error
        end

      error ->
        error
    end
  end

  defp register_as_job_definition(workflow_id, workflow_config) do
    main_node = workflow_config.nodes[:main]

    job_config = %{
      module: main_node[:module],
      function: main_node[:function] || :run,
      args: main_node[:args] || [],
      schedule: workflow_config[:schedule] || :manual,
      enable_progress: workflow_config[:enable_progress] || false,
      max_retries: workflow_config[:max_retries] || 3,
      timeout: workflow_config[:timeout] || :infinity
    }

    try do
      # Store in job_definitions table for persistence
      attrs = JobDefinition.from_config(workflow_id, job_config)

      %JobDefinition{}
      |> JobDefinition.changeset(attrs)
      |> Repo.insert(on_conflict: :replace_all, conflict_target: :job_id)

      Logger.debug("Registered job definition for #{workflow_id}")
      :ok
    rescue
      error ->
        Logger.warning("Failed to register job definition for #{workflow_id}: #{inspect(error)}")
        :ok  # Don't fail overall registration for database issues
    end
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

  defp is_single_node_workflow?(workflow_config) do
    nodes = workflow_config[:nodes] || %{}
    map_size(nodes) == 1 and Map.has_key?(nodes, :main)
  end

  defp ensure_modules_loaded do
    try do
      # Attempt to compile workflow files
      workflow_files = Path.wildcard("workflows/**/*.ex")
      Enum.each(workflow_files, fn file ->
        try do
          Code.compile_file(file)
        rescue
          _ -> :ok  # Ignore compilation errors for individual files
        end
      end)
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

  ## Validation Functions

  defp validate_required_fields(config) do
    required = [:nodes]
    missing = Enum.filter(required, fn field -> not Map.has_key?(config, field) end)

    case missing do
      [] -> :ok
      fields -> {:error, "Missing required fields: #{inspect(fields)}"}
    end
  end

  defp validate_nodes_structure(nodes) when is_map(nodes) do
    case Enum.find(nodes, fn {_node_id, node_config} ->
      not Map.has_key?(node_config, :module) or
      (not Map.has_key?(node_config, :function) and not is_single_main_node?(nodes))
    end) do
      nil -> :ok
      {node_id, _} -> {:error, "Node #{node_id} missing required module/function"}
    end
  end
  defp validate_nodes_structure(_), do: {:error, "Nodes must be a map"}

  defp is_single_main_node?(nodes) do
    map_size(nodes) == 1 and Map.has_key?(nodes, :main)
  end

  defp validate_workflow_module(workflow_config) do
    # For single-node workflows, verify the module/function exists
    if is_single_node_workflow?(workflow_config) do
      main_node = workflow_config.nodes[:main]
      module = main_node[:module]
      function = main_node[:function] || :run

      case Code.ensure_loaded(module) do
        {:module, ^module} ->
          if function_exported?(module, function, 1) or function_exported?(module, function, 0) do
            :ok
          else
            {:error, "Function #{module}.#{function}/0 or #{module}.#{function}/1 not found"}
          end
        {:error, reason} ->
          {:error, "Module #{module} not found: #{reason}"}
      end
    else
      # For multi-node workflows, validate each node's module/function
      validate_dag_nodes(workflow_config.nodes)
    end
  end

  defp validate_dag_nodes(nodes) do
    case Enum.find(nodes, fn {_node_id, node_config} ->
      module = node_config[:module]
      function = node_config[:function]

      # Check if module and function are present and valid
      cond do
        is_nil(module) ->
          true  # Missing module - this is invalid
        is_nil(function) ->
          true  # Missing function - this is invalid
        not is_atom(module) ->
          true  # Module should be atom - this is invalid
        not is_atom(function) ->
          true  # Function should be atom - this is invalid
        true ->
          # Check if module/function exists and is callable
          case Code.ensure_loaded(module) do
            {:module, ^module} ->
              # Check if function is exported with 0 or 1 arity
              not (function_exported?(module, function, 0) or function_exported?(module, function, 1))
            _ ->
              true  # Module doesn't exist - this is invalid
          end
      end
    end) do
      nil ->
        :ok  # No invalid nodes found
      {node_id, node_config} ->
        module = node_config[:module] || "unknown"
        function = node_config[:function] || "unknown"
        {:error, "Node #{node_id}: #{module}.#{function} not found or invalid"}
    end
  end

  defp try_dry_run(workflow_id, workflow_config) do
    if is_single_node_workflow?(workflow_config) do
      main_node = workflow_config.nodes[:main]
      module = main_node[:module]
      function = main_node[:function] || :run

      # Test that we can call the function (but don't actually run it)
      case Code.ensure_loaded(module) do
        {:module, ^module} ->
          if function_exported?(module, function, 1) or function_exported?(module, function, 0) do
            {:ok, "Workflow #{workflow_id} appears valid and executable"}
          else
            {:error, "Function #{module}.#{function} not exported"}
          end
        error ->
          {:error, "Cannot load module: #{inspect(error)}"}
      end
    else
      # For DAG workflows, validate structure and dependencies
      case validate_dag_structure(workflow_config.nodes) do
        :ok -> {:ok, "Multi-node workflow #{workflow_id} structure appears valid"}
        error -> error
      end
    end
  end

  defp validate_dag_structure(nodes) when is_map(nodes) do
    # Check for cycles and validate dependencies
    node_ids = Map.keys(nodes) |> MapSet.new()

    # Validate all dependencies exist
    case Enum.find(nodes, fn {_node_id, node_config} ->
      depends_on = node_config[:depends_on] || []
      not Enum.all?(depends_on, &MapSet.member?(node_ids, &1))
    end) do
      {node_id, _config} ->
        {:error, "Node #{node_id} has invalid dependencies"}

      nil ->
        # Check for cycles using DFS
        case detect_cycles(nodes) do
          nil -> :ok
          cycle -> {:error, "Cycle detected: #{inspect(cycle)}"}
        end
    end
  end
  defp validate_dag_structure(_), do: {:error, "Nodes must be a map"}

  defp detect_cycles(nodes) do
    visited = MapSet.new()
    rec_stack = MapSet.new()

    Enum.find_value(Map.keys(nodes), fn node_id ->
      if not MapSet.member?(visited, node_id) do
        case dfs_cycle_check(node_id, nodes, visited, rec_stack, []) do
          {:cycle, path} -> path
          :no_cycle -> nil
        end
      end
    end)
  end

  defp dfs_cycle_check(node_id, nodes, visited, rec_stack, path) do
    if MapSet.member?(rec_stack, node_id) do
      {:cycle, Enum.reverse([node_id | path])}
    else
      if not MapSet.member?(visited, node_id) do
        visited = MapSet.put(visited, node_id)
        rec_stack = MapSet.put(rec_stack, node_id)
        new_path = [node_id | path]

        node_config = Map.get(nodes, node_id, %{})
        depends_on = node_config[:depends_on] || []

        case Enum.find_value(depends_on, fn dep ->
          case dfs_cycle_check(dep, nodes, visited, rec_stack, new_path) do
            {:cycle, cycle_path} -> {:cycle, cycle_path}
            :no_cycle -> nil
          end
        end) do
          {:cycle, cycle_path} -> {:cycle, cycle_path}
          nil -> :no_cycle
        end
      else
        :no_cycle
      end
    end
  end
end
