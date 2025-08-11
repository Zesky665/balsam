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
        :timer.sleep(100)
        register_etl_jobs()  # Load from external config
        Logger.info("Balsam Application started successfully")
        {:ok, pid}
      error ->
        error
    end
  end

  defp register_etl_jobs do
    try do
      Application.get_env(:balsam, :etl_jobs, [])
      |> Enum.each(fn job_config ->
        Balsam.Orchestrator.register_job(job_config.id, Map.delete(job_config, :id))
      end)

      Logger.info("ETL jobs registered successfully")
    rescue
      error ->
        Logger.error("Failed to register ETL jobs: #{inspect(error)}")
    end
  end
end
