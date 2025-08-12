# lib/balsam/startup_job_loader.ex
defmodule Balsam.StartupJobLoader do
  @moduledoc """
  Handles loading and registering ETL jobs at application startup.
  This addresses timing issues with module compilation and loading.
  """

  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Schedule job registration after the system fully starts
    send(self(), :register_jobs)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:register_jobs, state) do
    Logger.info("Starting automatic ETL job registration...")

    try do
      # First, explicitly load known ETL modules
      load_etl_modules()

      # Wait a bit for modules to fully initialize
      :timer.sleep(200)

      # Try registration
      case attempt_job_registration() do
        :ok ->
          Logger.info("✅ ETL jobs registered successfully at startup")
        {:error, reason} ->
          Logger.warning("⚠️  Initial job registration failed: #{reason}")
          # Schedule a retry
          Process.send_after(self(), :retry_registration, 2000)
      end
    rescue
      error ->
        Logger.error("❌ Error during job registration: #{inspect(error)}")
        # Schedule a retry
        Process.send_after(self(), :retry_registration, 2000)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:retry_registration, state) do
    Logger.info("Retrying ETL job registration...")

    case attempt_job_registration() do
      :ok ->
        Logger.info("✅ ETL jobs registered successfully on retry")
      {:error, reason} ->
        Logger.warning("⚠️  Job registration retry failed: #{reason}")
        Logger.info("💡 You can manually register with: Balsam.JobRegistry.register_all()")
    end

    {:noreply, state}
  end

  defp load_etl_modules do
    # Explicitly load modules we know should exist
    known_etl_modules = [
      ETL.SimpleTestJob,
      ETL.Analytics.UserBehavior
    ]

    Enum.each(known_etl_modules, fn module ->
      case Code.ensure_loaded(module) do
        {:module, ^module} ->
          Logger.debug("✅ Loaded ETL module: #{module}")
        {:error, reason} ->
          Logger.debug("⚠️  Could not load ETL module #{module}: #{reason}")
      end
    end)

    # Also try to load any modules from the etl/ directory
    load_etl_directory_modules()
  end

  defp load_etl_directory_modules do
    # Get all beam files that might be ETL modules
    case :code.get_path() do
      paths when is_list(paths) ->
        paths
        |> Enum.flat_map(&find_etl_beam_files/1)
        |> Enum.uniq()
        |> Enum.each(&try_load_beam_module/1)
      _ -> :ok
    end
  end

  defp find_etl_beam_files(path) do
    case File.ls(path) do
      {:ok, files} ->
        files
        |> Enum.filter(fn file ->
          String.ends_with?(file, ".beam") and
          (String.contains?(file, "ETL") or String.contains?(file, "Etl"))
        end)
        |> Enum.map(fn file ->
          # Convert beam filename to module name
          file
          |> String.replace(".beam", "")
          |> String.to_atom()
        end)
      _ -> []
    end
  end

  defp try_load_beam_module(module_atom) do
    try do
      Code.ensure_loaded(module_atom)
    rescue
      _ -> :ok
    end
  end

  defp attempt_job_registration do
    # Discover ETL jobs
    jobs = Balsam.JobRegistry.discover_etl_jobs()

    if length(jobs) == 0 do
      available_modules = get_available_etl_modules()
      {:error, "No ETL jobs discovered. Available ETL modules: #{inspect(available_modules)}"}
    else
      # Register discovered jobs
      Enum.each(jobs, fn {job_id, job_config} ->
        case Balsam.Orchestrator.register_job(job_id, job_config) do
          :ok ->
            Logger.info("📝 Registered ETL job: #{job_id}")
          {:error, reason} ->
            Logger.error("❌ Failed to register job #{job_id}: #{inspect(reason)}")
        end
      end)

      Logger.info("🎉 ETL job registration complete: #{length(jobs)} jobs processed")
      :ok
    end
  end

  defp get_available_etl_modules do
    :code.all_loaded()
    |> Enum.filter(fn {module_name, _} ->
      module_string = Atom.to_string(module_name)
      String.contains?(module_string, "ETL")
    end)
    |> Enum.map(fn {module_name, _} -> module_name end)
  end
end
