defmodule ETL.SimpleTestWorkflow do
  @behaviour Balsam.Workflow

  @impl true
  def workflow_config do
    %{
      name: "Simple Test Workflow",
      description: "A basic test to verify the unified system works",
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
    IO.puts("🚀 Starting simple test workflow...")

    if progress_callback do
      progress_callback.(1, 4, "Initializing")
      :timer.sleep(500)

      progress_callback.(2, 4, "Processing data")
      :timer.sleep(500)

      progress_callback.(3, 4, "Finishing up")
      :timer.sleep(500)

      progress_callback.(4, 4, "Complete!")
    end

    IO.puts("✅ Simple test workflow completed successfully!")
    {:ok, %{message: "Test completed", timestamp: DateTime.utc_now()}}
  end
end
