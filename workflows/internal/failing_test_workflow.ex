# workflows/test/failing_test_workflow.ex
defmodule Workflows.Internal.FailingTestWorkflow do
  @behaviour Balsam.Workflow

  @moduledoc """
  A workflow designed to fail for testing error handling in the registry.
  """

  @impl true
  def workflow_config do
    %{
      name: "Failing Test Workflow",
      description: "Intentionally fails to test error handling",
      schedule: :manual,
      nodes: %{
        main: %{
          module: __MODULE__,
          function: :run_and_fail,
          args: [],
          depends_on: []
        }
      }
    }
  end

  def run_and_fail(_progress_callback \\ nil) do
    # This function will raise an error when called
    raise "Intentional test failure"
  end
end
