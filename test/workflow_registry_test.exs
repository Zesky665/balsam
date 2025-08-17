defmodule WorkflowRegistryTest do
  use ExUnit.Case
  doctest Balsam.WorkflowRegistry

  test "discovers workflow modules" do
    workflows = Balsam.WorkflowRegistry.discover_workflows()

    # Should find our test workflows
    workflow_ids = Enum.map(workflows, fn {id, _} -> id end)

    # These might be different based on what files exist
    assert length(workflow_ids) > 0

    # Check if at least one of our expected workflows exists
    expected_workflows = [:simple_test_workflow, :analytics_user_behavior, :pipeline_test_pipeline]
    found_any = Enum.any?(expected_workflows, fn id -> id in workflow_ids end)
    assert found_any, "Expected to find at least one workflow, found: #{inspect(workflow_ids)}"
  end

  test "workflow configs are properly formatted" do
    workflows = Balsam.WorkflowRegistry.discover_workflows()

    Enum.each(workflows, fn {_workflow_id, config} ->
      # Each workflow should have required fields
      assert is_binary(config[:name])
      assert is_map(config[:nodes])
      assert map_size(config[:nodes]) > 0

      # Each node should have required fields
      Enum.each(config[:nodes], fn {_node_id, node_config} ->
        assert is_atom(node_config[:module])
        assert is_atom(node_config[:function])
        assert is_list(node_config[:depends_on])
      end)
    end)
  end
end
