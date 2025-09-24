defmodule TestWorkflows do
  def run_tests do
    IO.puts("\n🧪 Testing Unified Workflow System")
    IO.puts("=" |> String.duplicate(50))

    # Test 1: Discovery
    test_discovery()

    # Test 2: Registration
    test_registration()

    # Test 3: Run simple workflow
    test_simple_workflow()

    # Test 4: Run pipeline
    test_pipeline()

    IO.puts("\n🎉 All tests completed!")
  end

  defp test_discovery do
    IO.puts("\n1. Testing workflow discovery...")

    workflows = Balsam.WorkflowRegistry.discover_workflows()
    IO.puts("   Discovered #{length(workflows)} workflows:")

    Enum.each(workflows, fn {id, config} ->
      node_count = map_size(config[:nodes])
      type = if node_count == 1, do: "job", else: "DAG"
      IO.puts("   - #{id} (#{type}, #{node_count} nodes)")
    end)
  end

  defp test_registration do
    IO.puts("\n2. Testing workflow registration...")

    try do
      Balsam.WorkflowRegistry.register_all()
      IO.puts("   ✅ Registration completed successfully")
    rescue
      error ->
        IO.puts("   ❌ Registration failed: #{inspect(error)}")
    end
  end

  defp test_simple_workflow do
    IO.puts("\n3. Testing simple workflow execution...")

    case Balsam.Orchestrator.run_dag(:simple_test_workflow) do
      {:ok, run_id} ->
        IO.puts("   ✅ Simple workflow started with run_id: #{run_id}")

        # Wait a bit and check status
        :timer.sleep(3000)
        case Balsam.Orchestrator.dag_status(:simple_test_workflow) do
          {:ok, status} ->
            IO.puts("   Status: #{inspect(status[:status])}")
          error ->
            IO.puts("   Status check failed: #{inspect(error)}")
        end

      error ->
        IO.puts("   ❌ Failed to start simple workflow: #{inspect(error)}")
    end
  end

  defp test_pipeline do
    IO.puts("\n4. Testing pipeline execution...")

    case Balsam.Orchestrator.run_dag(:pipeline_test_pipeline) do
      {:ok, run_id} ->
        IO.puts("   ✅ Pipeline started with run_id: #{run_id}")

        # Monitor progress
        monitor_pipeline(5)

      error ->
        IO.puts("   ❌ Failed to start pipeline: #{inspect(error)}")
    end
  end

  defp monitor_pipeline(0) do
    IO.puts("   ⏰ Monitoring timeout")
  end

  defp monitor_pipeline(tries_left) do
    :timer.sleep(2000)

    case Balsam.Orchestrator.dag_status(:pipeline_test_pipeline) do
      {:ok, %{status: :running} = status} ->
        IO.puts("   📊 Pipeline running - completed: #{length(status[:completed_nodes] || [])}")
        monitor_pipeline(tries_left - 1)

      {:ok, %{status: status}} ->
        IO.puts("   🏁 Pipeline finished with status: #{status}")

      error ->
        IO.puts("   ❌ Status check failed: #{inspect(error)}")
    end
  end
end

# Run the tests if this file is executed directly
if __ENV__.file == "test_workflows.exs" do
  TestWorkflows.run_tests()
end
