# workflows/test/simple_test_job.ex
defmodule Workflows.Test.SimpleTestJob do
  @behaviour Balsam.Workflow

  @moduledoc """
  A simple single-node workflow for testing job registration.
  """

  @impl true
  def workflow_config do
    %{
      name: "Simple Test Job",
      description: "A basic single-node workflow for testing",
      schedule: :manual,
      enable_progress: true,
      max_retries: 2,
      timeout: :timer.minutes(5),
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
      progress_callback.(1, 3, "Starting simple test job")
      :timer.sleep(100)

      progress_callback.(2, 3, "Processing test data")
      :timer.sleep(100)

      progress_callback.(3, 3, "Completing test job")
      :timer.sleep(100)
    end

    {:ok, %{
      message: "Simple test job completed successfully",
      timestamp: DateTime.utc_now(),
      data_processed: 42
    }}
  end
end
