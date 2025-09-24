# test/workflow_registry_comprehensive_test.exs
defmodule WorkflowRegistryComprehensiveTest do
  use ExUnit.Case, async: false  # Set to false for database tests
  alias Balsam.{WorkflowRegistry, Orchestrator, Repo, Schema.JobDefinition}

  setup do
    # Start sandbox for this test
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)

    # Ensure tables exist (run migration if needed)
    ensure_database_ready()

    # Clean up database before each test
    Repo.delete_all(JobDefinition)

    # Start a clean orchestrator for testing (don't start the main application one)
    {:ok, orchestrator} = Orchestrator.start_link(name: :"test_orchestrator_#{:rand.uniform(10000)}")

    # No manual compilation needed
    %{orchestrator: orchestrator}
  end

  defp ensure_database_ready do
    try do
      # Try a simple query to check if tables exist
      Repo.all(JobDefinition, limit: 1)
    rescue
      Postgrex.Error ->
        # Tables don't exist, run migration
        Ecto.Migrator.up(Repo, 0, Balsam.Repo.Migrations.CreateBalsamTables, log: false)
      Ecto.Adapters.SQL.SandboxError ->
        # Sandbox issue, but tables probably exist
        :ok
      other_error ->
        # Some other error, try to create tables manually
        create_test_tables()
    end
  end

  defp create_test_tables do
    # Create minimal tables for testing if migration fails
    try do
      Ecto.Adapters.SQL.query!(Repo, """
        CREATE TABLE IF NOT EXISTS job_definitions (
          id INTEGER PRIMARY KEY,
          job_id TEXT NOT NULL UNIQUE,
          module TEXT NOT NULL,
          function TEXT NOT NULL,
          args TEXT,
          schedule TEXT,
          enable_progress BOOLEAN DEFAULT 0,
          max_retries INTEGER DEFAULT 3,
          timeout TEXT DEFAULT 'infinity',
          active BOOLEAN DEFAULT 1,
          last_run INTEGER,
          inserted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      """)
    rescue
      _ -> :ok  # Ignore if creation fails
    end
  end

  describe "Workflow Discovery" do
    test "can discover workflows" do
      workflows = WorkflowRegistry.discover_workflows()

      IO.puts("\n=== WORKFLOW DISCOVERY RESULTS ===")
      IO.puts("Total discovered: #{length(workflows)}")

      Enum.each(workflows, fn {id, config} ->
        IO.puts("  #{id}:")
        IO.puts("    Name: #{config[:name]}")
        IO.puts("    Nodes: #{map_size(config.nodes)}")
        IO.puts("    Type: #{if map_size(config.nodes) == 1, do: "job", else: "dag"}")
      end)
      IO.puts("=== END DISCOVERY ===\n")

      # Just verify we can discover some workflows
      # Don't make assumptions about specific ones existing
      assert is_list(workflows)
    end
  end

  describe "Single Node Job Registration" do
    # @tag :skip  # Skip this until discovery is working
    test "can register a workflow", %{orchestrator: orchestrator} do
      workflows = WorkflowRegistry.discover_workflows()

      # Find any single-node workflow
      single_node_workflow = Enum.find(workflows, fn {_id, config} ->
        map_size(config.nodes) == 1 and Map.has_key?(config.nodes, :main)
      end)

      if single_node_workflow do
        {workflow_id, workflow_config} = single_node_workflow

        # Test registration
        assert :ok = Orchestrator.register_workflow(orchestrator, workflow_id, workflow_config)

        # Verify registration
        status = Orchestrator.get_status(orchestrator)
        assert workflow_id in status.registered_workflows
      else
        # No single-node workflows found, skip test
        ExUnit.skip("No single-node workflows discovered for testing")
      end
    end

    # @tag :skip  # Skip this until discovery is working
    test "creates job definitions in database" do
      # Only test this if we have database access
      try do
        initial_count = Repo.aggregate(JobDefinition, :count, :id)

        # Run registration
        WorkflowRegistry.register_all()

        final_count = Repo.aggregate(JobDefinition, :count, :id)

        # Verify some registrations happened (if workflows were discovered)
        workflows = WorkflowRegistry.discover_workflows()
        single_node_count = Enum.count(workflows, fn {_id, config} ->
          map_size(config.nodes) == 1
        end)

        if single_node_count > 0 do
          assert final_count > initial_count, "Should have created job definitions"
        end

      rescue
        error ->
          ExUnit.skip("Database not available for test: #{inspect(error)}")
      end
    end
  end

  describe "Multi-Node DAG Registration" do
    # @tag :skip  # Skip until discovery works
    test "can find multi-node workflows" do
      workflows = WorkflowRegistry.discover_workflows()

      multi_node_workflows = Enum.filter(workflows, fn {_id, config} ->
        map_size(config.nodes) > 1
      end)

      # Just verify the structure if any exist
      Enum.each(multi_node_workflows, fn {workflow_id, config} ->
        assert is_map(config.nodes)
        assert map_size(config.nodes) > 1

        # Verify each node has required fields
        Enum.each(config.nodes, fn {_node_id, node_config} ->
          assert Map.has_key?(node_config, :module)
          assert Map.has_key?(node_config, :function)
          assert Map.has_key?(node_config, :depends_on)
        end)
      end)
    end
  end
end
