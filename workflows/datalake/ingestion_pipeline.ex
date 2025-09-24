defmodule Workflows.DataLake.IngestionPipeline do
  @behaviour Balsam.Workflow  # Change from Balsam.ETLJob

  @impl true
  def workflow_config do  # Change from job_config
    %{
      name: "Data Lake Ingestion Pipeline",
      description: "Ingest data into the data lake",
      schedule: {:interval, :timer.hours(2)},
      max_concurrent_nodes: 1,
      enable_progress: true,
      nodes: %{
        main: %{
          module: __MODULE__,
          function: :run,
          args: [],
          depends_on: []
        }
      }
    }
  end

  @impl true
  def run(progress_callback \\ nil) do
    IO.puts("🏗️ Starting data lake ingestion...")

    if progress_callback do
      progress_callback.(1, 3, "Connecting to sources")
      :timer.sleep(1000)

      progress_callback.(2, 3, "Ingesting data")
      :timer.sleep(2000)

      progress_callback.(3, 3, "Finalizing ingestion")
      :timer.sleep(500)
    end

    IO.puts("✅ Data lake ingestion completed!")
    {:ok, %{records_ingested: 10000}}
  end
end
