defmodule ETL.SimpleTestJob do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "Simple Test Job",
      description: "A basic test job",
      schedule: :manual,
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
    if progress_callback do
      progress_callback.(1, 3, "Starting test job")
      :timer.sleep(1000)
      progress_callback.(2, 3, "Processing")
      :timer.sleep(1000)
      progress_callback.(3, 3, "Completed")
    end

    {:ok, "Test job completed successfully"}
  end
end
