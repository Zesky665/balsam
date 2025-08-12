defmodule Balsam.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting Balsam Application")

    children = [
      # Start Ecto repository first
      Balsam.Repo,

      # Start the job orchestrator
      {Balsam.Orchestrator, [
        name: Balsam.Orchestrator,
        max_concurrent_jobs: 5
      ]},

      # Start the DAG orchestrator
      {Balsam.DagOrchestrator, [
        name: Balsam.DagOrchestrator,
        orchestrator_pid: Balsam.Orchestrator,
        max_concurrent_dags: 2
      ]}
    ]

    opts = [strategy: :one_for_one, name: Balsam.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        :timer.sleep(100)
        # Load jobs and DAGs from configuration modules
        Balsam.JobRegistry.register_all()
        Balsam.DagRegistry.register_all()
        Logger.info("Balsam Application started successfully")
        {:ok, pid}
      error ->
        error
    end
  end
end
