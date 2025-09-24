defmodule Balsam.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting Balsam Application with enhanced fault tolerance")

    children = [
      # Database first
      Balsam.Repo,

      # Registries for job tracking
      Balsam.JobRegistry,

      # Supervisors for job and DAG processes
      {Balsam.JobSupervisor, []},
      {Balsam.DagSupervisor, []},

      # Main orchestrator (fault-tolerant)
      {Balsam.Orchestrator, [
        name: Balsam.Orchestrator,
        max_concurrent: 5
      ]},

      # Startup loader (registers workflows after everything is ready)
      Balsam.StartupLoader
    ]

    opts = [
      strategy: :one_for_one,
      name: Balsam.Supervisor,
      max_restarts: 10,
      max_seconds: 60
    ]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("Balsam Application started successfully with fault tolerance")
        {:ok, pid}
      error ->
        Logger.error("Failed to start Balsam Application: #{inspect(error)}")
        error
    end
  end
end

defmodule Balsam.HealthChecker do
  @moduledoc """
  Monitors system health and can trigger recovery actions.
  """

  use GenServer
  require Logger

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Schedule periodic health checks
    schedule_health_check()
    {:ok, %{last_check: DateTime.utc_now()}}
  end

  @impl true
  def handle_info(:health_check, state) do
    perform_health_check()
    schedule_health_check()
    {:noreply, %{state | last_check: DateTime.utc_now()}}
  end

  defp schedule_health_check do
    Process.send_after(self(), :health_check, :timer.minutes(5))
  end

  defp perform_health_check do
    try do
      # Check orchestrator health
      case Balsam.Orchestrator.get_status() do
        status when is_map(status) ->
          case status.system_health do
            :unhealthy ->
              Logger.warning("System health is unhealthy - consider intervention")
            :degraded ->
              Logger.info("System health is degraded - monitoring closely")
            _ ->
              Logger.debug("System health check: OK")
          end
        _ ->
          Logger.error("Could not get orchestrator status")
      end

      # Check database connectivity
      case Balsam.Repo.query("SELECT 1", []) do
        {:ok, _} -> :ok
        error -> Logger.error("Database health check failed: #{inspect(error)}")
      end

    rescue
      error ->
        Logger.error("Health check failed: #{inspect(error)}")
    end
  end
end
