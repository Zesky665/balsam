defmodule Balsam.JobRegistry do
  @moduledoc """
  Centralized registry for all ETL jobs.
  This module handles automatic discovery and registration of ETL jobs
  from the etl/ directory that can be used standalone or as nodes in DAGs.
  """

  require Logger

  @doc """
  Register all ETL jobs with the orchestrator.
  Automatically discovers jobs in the etl/ directory that implement
  the ETL job behavior.
  """
  def register_all do
    try do
      # Ensure ETL modules are loaded before discovery
      ensure_etl_modules_loaded()

      # Discover and register ETL jobs from the etl/ directory
      etl_jobs = discover_etl_jobs()

      if length(etl_jobs) == 0 do
        Logger.warning("No ETL jobs discovered. Make sure ETL modules are compiled and implement @behaviour Balsam.ETLJob")
        Logger.info("Available modules with ETL in name: #{inspect(get_etl_module_names())}")
      end

      Enum.each(etl_jobs, fn {job_id, job_config} ->
        case Balsam.Orchestrator.register_job(job_id, job_config) do
          :ok ->
            Logger.info("Registered ETL job: #{job_id}")
          {:error, reason} ->
            Logger.error("Failed to register job #{job_id}: #{inspect(reason)}")
        end
      end)

      Logger.info("ETL job registration complete: #{length(etl_jobs)} jobs processed")
    rescue
      error ->
        Logger.error("Failed to register ETL jobs: #{inspect(error)}")
        Logger.info("You can retry with: Balsam.JobRegistry.register_all()")
    end
  end

  @doc """
  Manually register a specific ETL job.
  Useful for testing or when you need to override auto-discovery.
  """
  def register_job(job_id, job_config) do
    case Balsam.Orchestrator.register_job(job_id, job_config) do
      :ok ->
        Logger.info("Manually registered ETL job: #{job_id}")
        :ok
      error ->
        Logger.error("Failed to manually register job #{job_id}: #{inspect(error)}")
        error
    end
  end

  ## Private Functions

  @doc false
  def discover_etl_jobs do
    # Get all compiled modules that implement the Balsam.ETLJob behavior
    :code.all_loaded()
    |> Enum.filter(&etl_job_module?/1)
    |> Enum.map(&extract_job_config/1)
    |> Enum.filter(&(&1 != nil))
    |> Enum.sort_by(fn {job_id, _config} -> job_id end)
  end

  defp etl_job_module?({module_name, _loaded_from}) do
    try do
      # Check if module implements Balsam.ETLJob behavior
      implements_etl_behavior?(module_name) and
      # Make sure it's not a test or support module
      not is_test_or_support_module?(module_name)
    rescue
      _ -> false
    end
  end

  defp implements_etl_behavior?(module_name) do
    # Check if module implements the ETL job behavior
    try do
      case module_name.module_info(:attributes) do
        attributes when is_list(attributes) ->
          behaviors = Keyword.get_values(attributes, :behaviour) ++
                     Keyword.get_values(attributes, :behavior)  # American spelling
          behaviors = List.flatten(behaviors)
          Balsam.ETLJob in behaviors
        _ -> false
      end
    rescue
      _ -> false
    end
  end

  defp is_test_or_support_module?(module_name) do
    module_string = Atom.to_string(module_name)

    # Be more specific about what we filter out
    String.ends_with?(module_string, "Test") or
    String.ends_with?(module_string, "TestCase") or
    String.contains?(module_string, "Test.") or
    String.contains?(module_string, ".Test.") or
    String.contains?(module_string, "Support") or
    String.ends_with?(module_string, ".Helper") or
    String.ends_with?(module_string, ".Utils") or
    String.contains?(module_string, "ExUnit")
  end

  defp extract_job_config({module_name, _}) do
    try do
      # Since we already know this implements Balsam.ETLJob, we can safely extract config
      job_id = generate_job_id(module_name)

      # Get job configuration from the module
      job_config = try do
        module_config = apply(module_name, :job_config, [])
        Balsam.ETLJob.merge_config(module_config)
      rescue
        _ -> Balsam.ETLJob.default_config()
      end

      # Set up the base configuration for the orchestrator
      orchestrator_config = %{
        module: module_name,
        function: :run,
        args: [],
        enable_progress: Map.get(job_config, :enable_progress, false),
        max_retries: Map.get(job_config, :max_retries, 3),
        timeout: Map.get(job_config, :timeout, :timer.minutes(10)),
        schedule: Map.get(job_config, :schedule, :manual)
      }

      {job_id, orchestrator_config}
    rescue
      error ->
        Logger.warning("Failed to extract config for #{module_name}: #{inspect(error)}")
        nil
    end
  end

  defp generate_job_id(module_name) do
    module_name
    |> Atom.to_string()
    |> String.replace("Elixir.", "")
    |> String.replace("ETL.", "")
    |> String.replace("Pipeline.", "")
    # Convert CamelCase to snake_case and handle nested modules
    |> String.replace(~r/([a-z])([A-Z])/, "\\1_\\2")
    |> String.downcase()
    |> String.replace(".", "_")
    |> String.replace(~r/[^a-z0-9_]/, "_")
    |> String.replace(~r/_+/, "_")
    |> String.trim("_")
    |> String.to_atom()
  end

  @doc """
  Get information about all discovered ETL jobs.
  Useful for debugging and monitoring.
  """
  def list_discovered_jobs do
    discover_etl_jobs()
    |> Enum.map(fn {job_id, config} ->
      %{
        job_id: job_id,
        module: config.module,
        function: config.function,
        has_progress: config.enable_progress,
        has_custom_config: function_exported?(config.module, :job_config, 0)
      }
    end)
  end

  defp ensure_etl_modules_loaded do
    # Force compilation and loading of ETL modules
    try do
      # Recompile the etl directory if needed
      Code.compile_file("etl/**/*.ex")
    rescue
      _ -> :ok  # Files might not exist or already be compiled
    end

    # Give the system time to load modules
    :timer.sleep(100)
  end

  defp get_etl_module_names do
    :code.all_loaded()
    |> Enum.filter(fn {module_name, _} ->
      module_string = Atom.to_string(module_name)
      String.contains?(module_string, "ETL")
    end)
    |> Enum.map(fn {module_name, _} -> module_name end)
  end
end
