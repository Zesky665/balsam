defmodule Balsam.Application do
  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    Logger.info("Starting Balsam Application")

    children = [
      Balsam.Repo,
      {Balsam.Orchestrator, [  # This is the renamed DagOrchestrator
        name: Balsam.Orchestrator,
        max_concurrent_dags: 5
      ]},
      Balsam.StartupLoader
    ]

    opts = [strategy: :one_for_one, name: Balsam.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        Logger.info("Balsam Application started successfully")
        {:ok, pid}
      error ->
        error
    end
  end
end
