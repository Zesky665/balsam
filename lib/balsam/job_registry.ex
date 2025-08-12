defmodule Balsam.JobRegistry do
  @moduledoc """
  Centralized registry for all ETL jobs.
  This module handles registration of individual jobs that can be used
  standalone or as nodes in DAGs.
  """

  require Logger

  @doc """
  Register all ETL jobs with the orchestrator.
  """
  def register_all do
    try do
      # Register your existing jokes ETL job
      Balsam.Orchestrator.register_job(:jokes_etl, %{
        module: JokesETL,
        function: :main,
        args: [],
        enable_progress: false
      })

      # Add more jobs here as you create them
      # Balsam.Orchestrator.register_job(:my_new_job, %{
      #   module: MyModule,
      #   function: :my_function,
      #   args: [],
      #   enable_progress: false
      # })

      Logger.info("ETL jobs registered successfully")
    rescue
      error ->
        Logger.error("Failed to register ETL jobs: #{inspect(error)}")
    end
  end
end
