# workflows/test/invalid_test_workflow.ex
defmodule Workflows.Test.InvalidTestWorkflow do
  @behaviour Balsam.Workflow

  @moduledoc """
  An invalid workflow configuration for testing validation logic.
  """

  @impl true
  def workflow_config do
    %{
      name: "Invalid Test Workflow",
      description: "Has invalid configuration for testing validation",
      schedule: :manual,
      nodes: %{
        broken_node: %{
          module: NonExistentModule,  # This module doesn't exist
          function: :missing_function,
          args: [],
          depends_on: []
        },
        circular_dep: %{
          module: __MODULE__,
          function: :some_function,
          args: [],
          depends_on: [:another_circular_dep]  # Will create a cycle
        },
        another_circular_dep: %{
          module: __MODULE__,
          function: :another_function,
          args: [],
          depends_on: [:circular_dep]  # Completes the cycle
        }
      }
    }
  end

  # These functions don't actually need to work since the validation should catch the issues
  def some_function(_), do: :ok
  def another_function(_), do: :ok
end
