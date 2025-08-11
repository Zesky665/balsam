defmodule Balsam.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting Balsam Application")

    children = [
      # Start Ecto repository first
      Balsam.Repo,

      # Start the orchestrator
      {Balsam.Orchestrator, [
        name: Balsam.Orchestrator,
        max_concurrent_jobs: 3
      ]}
    ]

    opts = [strategy: :one_for_one, name: Balsam.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Register default jobs after everything starts
        :timer.sleep(100)  # Give the orchestrator a moment to fully initialize
        register_default_jobs()
        Logger.info("Balsam Application started successfully")
        {:ok, pid}

      error ->
        Logger.error("Failed to start Balsam Application: #{inspect(error)}")
        error
    end
  end

  defp register_default_jobs do
    try do
      # Register your original ETL job (quick, every 30 minutes)
      Balsam.Orchestrator.register_job(:jokes_etl, %{
        module: JokesETL,
        function: :main,
        args: [],
        schedule: {:interval, :timer.minutes(30)},
        enable_progress: false  # Disable progress to avoid complexity for now
      })

      # Register the long-running version (every 6 hours)
      Balsam.Orchestrator.register_job(:jokes_etl_long, %{
        module: JokesETL,
        function: :main_long_running,
        args: [],
        schedule: {:interval, :timer.hours(6)},
        enable_progress: true
      })

      # Register a manual cleanup job
      Balsam.Orchestrator.register_job(:cleanup_old_data, %{
        module: Balsam.JobStorage,
        function: :cleanup_old_data,
        args: [90],  # Keep 90 days of data
        schedule: :manual,
        enable_progress: false
      })

      Logger.info("Default ETL jobs registered successfully")
    rescue
      error ->
        Logger.error("Failed to register default jobs: #{inspect(error)}")
        Logger.info("You can register jobs manually using Balsam.Orchestrator.register_job/2")
    end
  end
end
