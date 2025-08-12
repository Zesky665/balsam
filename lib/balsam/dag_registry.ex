# lib/balsam/dag_registry.ex
defmodule Balsam.DagRegistry do
  @moduledoc """
  Centralized registry for all DAG definitions.
  This module handles registration of complex workflows composed of multiple jobs.
  """

  require Logger

  @doc """
  Register all DAG definitions with the DAG orchestrator.
  """
  def register_all do
    try do
      register_simple_jokes_dag()

      Logger.info("ETL DAGs registered successfully")
    rescue
      error ->
        Logger.error("Failed to register ETL DAGs: #{inspect(error)}")
    end
  end

  defp register_simple_jokes_dag do
    # Simple DAG using your existing jokes ETL
    jokes_dag = %{
      name: "Jokes ETL Pipeline",
      description: "Fetch and process programming jokes",
      schedule: {:interval, :timer.hours(6)}, # Run every 6 hours
      max_concurrent_nodes: 1,
      nodes: %{
        fetch_jokes: %{
          job_id: :jokes_etl,
          depends_on: []
        }
      }
    }

    Balsam.DagOrchestrator.register_dag(:jokes_pipeline, jokes_dag)
  end

  # Add more DAG definitions here as you build them
  # defp register_my_complex_dag do
  #   complex_dag = %{
  #     name: "My Complex Pipeline",
  #     description: "Multi-step data processing",
  #     schedule: :manual,
  #     max_concurrent_nodes: 2,
  #     nodes: %{
  #       step1: %{job_id: :job1, depends_on: []},
  #       step2: %{job_id: :job2, depends_on: [:step1]},
  #       step3: %{job_id: :job3, depends_on: [:step2]}
  #     }
  #   }
  #   Balsam.DagOrchestrator.register_dag(:my_pipeline, complex_dag)
  # end
end
