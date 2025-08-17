# test/workflow_registry_comprehensive_test.exs
defmodule WorkflowRegistryComprehensiveTest do
  use ExUnit.Case
  alias Balsam.{WorkflowRegistry, Orchestrator, Repo, Schema.JobDefinition}
  import Ecto.Query

  setup do
    # Clean up database before each test
    Repo.delete_all(JobDefinition)

    # Start a clean orchestrator for testing
    {:ok, orchestrator} = Orchestrator.start_link(name: :"test_orchestrator_#{:rand.uniform(10000)}")

    # Ensure test workflow modules are loaded
    Code.compile_file("workflows/test/simple_test_job.ex")
    Code.compile_file("workflows/test/data_pipeline_test.ex")
    Code.compile_file("workflows/test/failing_test_workflow.ex")
    Code.compile_file("workflows/test/invalid_test_workflow.ex")

    %{orchestrator: orchestrator}
  end

  describe "Single Node Job Registration" do
    test "can discover and register a simple job workflow", %{orchestrator: orchestrator} do
      # Discover workflows - should find our test job
      workflows = WorkflowRegistry.discover_workflows()

      # Find our simple test job
      simple_job = Enum.find(workflows, fn {id, _config} ->
        id == :test_simple_test_job
      end)

      assert simple_job != nil, "Should discover the simple test job"

      {workflow_id, workflow_config} = simple_job

      # Verify it's identified as a single-node workflow
      assert map_size(workflow_config.nodes) == 1
      assert Map.has_key?(workflow_config.nodes, :main)

      # Verify workflow config structure
      assert workflow_config.name == "Simple Test Job"
      assert workflow_config.description == "A basic single-node workflow for testing"
      assert workflow_config.schedule == :manual
      assert workflow_config.enable_progress == true
      assert workflow_config.max_retries == 2

      # Test registration with orchestrator
      assert :ok = Orchestrator.register_workflow(orchestrator, workflow_id, workflow_config)

      # Verify it was registered
      status = Orchestrator.get_status(orchestrator)
      assert workflow_id in status.registered_workflows
    end

    test "registers single-node workflows as job definitions in database", %{orchestrator: orchestrator} do
      # Get our simple job workflow
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_simple_test_job
      end)

      # Register using the complete registration process
      assert :ok = WorkflowRegistry.register_workflow_complete(workflow_id, workflow_config)

      # Check that it was stored in job_definitions table
      job_def = Repo.get_by(JobDefinition, job_id: to_string(workflow_id))
      assert job_def != nil
      assert job_def.module == "ETL.Test.SimpleTestJob"
      assert job_def.function == "run"
      assert job_def.enable_progress == true
      assert job_def.max_retries == 2
      assert job_def.active == true
    end

    test "can validate single-node workflow configuration" do
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_simple_test_job
      end)

      # Should pass validation
      assert :ok = WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config)

      # Test that it can find the module and function
      assert {:ok, message} = WorkflowRegistry.test_workflow(workflow_id)
      assert String.contains?(message, "appears valid and executable")
    end

    test "can execute dry run test of single-node workflow" do
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, _workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_simple_test_job
      end)

      # Test the workflow without actually running it
      case WorkflowRegistry.test_workflow(workflow_id) do
        {:ok, message} ->
          assert String.contains?(message, "appears valid")
        {:error, reason} ->
          flunk("Dry run should succeed, but got error: #{inspect(reason)}")
      end
    end
  end

  describe "Multi-Node DAG Registration" do
    test "can discover and register a multi-node DAG workflow", %{orchestrator: orchestrator} do
      # Discover workflows - should find our test DAG
      workflows = WorkflowRegistry.discover_workflows()

      # Find our test DAG
      test_dag = Enum.find(workflows, fn {id, _config} ->
        id == :test_data_pipeline_test
      end)

      assert test_dag != nil, "Should discover the test DAG"

      {workflow_id, workflow_config} = test_dag

      # Verify it's identified as a multi-node workflow
      assert map_size(workflow_config.nodes) == 5
      expected_nodes = [:extract_data, :validate_data, :transform_data, :load_data, :cleanup]
      Enum.each(expected_nodes, fn node ->
        assert Map.has_key?(workflow_config.nodes, node)
      end)

      # Verify workflow config structure
      assert workflow_config.name == "Test Data Pipeline"
      assert workflow_config.description == "Multi-step ETL pipeline for testing DAG functionality"
      assert workflow_config.max_concurrent_nodes == 2

      # Test registration with orchestrator
      assert :ok = Orchestrator.register_workflow(orchestrator, workflow_id, workflow_config)

      # Verify it was registered
      status = Orchestrator.get_status(orchestrator)
      assert workflow_id in status.registered_workflows
    end

    test "validates DAG dependencies correctly" do
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_data_pipeline_test
      end)

      # Should pass validation (no cycles, valid dependencies)
      assert :ok = WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config)

      # Verify dependency structure
      nodes = workflow_config.nodes

      # extract_data should have no dependencies
      assert nodes.extract_data.depends_on == []

      # validate_data should depend on extract_data
      assert nodes.validate_data.depends_on == [:extract_data]

      # transform_data should depend on validate_data
      assert nodes.transform_data.depends_on == [:validate_data]

      # load_data should depend on transform_data
      assert nodes.load_data.depends_on == [:transform_data]

      # cleanup should depend on load_data
      assert nodes.cleanup.depends_on == [:load_data]
    end

    test "validates all node functions exist" do
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_data_pipeline_test
      end)

      # Should pass validation - all functions should exist
      assert :ok = WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config)

      # Verify each node's function exists
      Enum.each(workflow_config.nodes, fn {_node_id, node_config} ->
        module = node_config.module
        function = node_config.function

        assert Code.ensure_loaded(module) == {:module, module}
        assert function_exported?(module, function, 0) or function_exported?(module, function, 1)
      end)
    end

    test "does not register multi-node workflows as job definitions" do
      workflows = WorkflowRegistry.discover_workflows()
      {workflow_id, workflow_config} = Enum.find(workflows, fn {id, _} ->
        id == :test_data_pipeline_test
      end)

      # Register the DAG
      assert :ok = WorkflowRegistry.register_workflow_complete(workflow_id, workflow_config)

      # Should NOT create a job definition (since it's multi-node)
      job_def = Repo.get_by(JobDefinition, job_id: to_string(workflow_id))
      assert job_def == nil, "Multi-node workflows should not create job definitions"
    end
  end

  describe "Validation and Error Handling" do
    test "detects invalid workflow configurations" do
      workflows = WorkflowRegistry.discover_workflows()

      # Find the invalid workflow
      invalid_workflow = Enum.find(workflows, fn {id, _} ->
        id == :test_invalid_test_workflow
      end)

      if invalid_workflow do
        {workflow_id, workflow_config} = invalid_workflow

        # Should fail validation due to non-existent module and circular dependencies
        case WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config) do
          {:error, reason} ->
            # Should detect the cycle or missing module
            assert String.contains?(reason, "Cycle detected") or
                   String.contains?(reason, "not found")
          :ok ->
            flunk("Should have failed validation due to invalid configuration")
        end
      end
    end

    test "handles workflows with missing functions gracefully" do
      # Test with a workflow that has a function that doesn't exist
      invalid_config = %{
        name: "Test Invalid Function",
        nodes: %{
          main: %{
            module: ETL.Test.SimpleTestJob,
            function: :non_existent_function,  # This function doesn't exist
            args: [],
            depends_on: []
          }
        }
      }

      case WorkflowRegistry.validate_workflow_config(:test_invalid, invalid_config) do
        {:error, reason} ->
          assert String.contains?(reason, "not found")
        :ok ->
          flunk("Should have failed validation due to missing function")
      end
    end

    test "handles missing required fields" do
      # Test with config missing required nodes field
      invalid_config = %{
        name: "Missing Nodes Config"
        # Missing :nodes field
      }

      case WorkflowRegistry.validate_workflow_config(:test_missing_nodes, invalid_config) do
        {:error, reason} ->
          assert String.contains?(reason, "Missing required fields")
        :ok ->
          flunk("Should have failed validation due to missing nodes")
      end
    end
  end

  describe "Complete Registration Process" do
    test "register_all() discovers and registers all valid workflows", %{orchestrator: orchestrator} do
      # Get initial count
      initial_status = Orchestrator.get_status(orchestrator)
      initial_count = length(initial_status.registered_workflows)

      # Run the complete registration process
      WorkflowRegistry.register_all()

      # Check that workflows were registered
      final_status = Orchestrator.get_status(orchestrator)
      final_count = length(final_status.registered_workflows)

      # Should have registered at least our test workflows
      assert final_count > initial_count

      # Check for specific test workflows
      registered_workflows = final_status.registered_workflows

      # Should find our simple job
      assert Enum.any?(registered_workflows, fn id ->
        Atom.to_string(id) |> String.contains?("simple_test_job")
      end)

      # Should find our test DAG
      assert Enum.any?(registered_workflows, fn id ->
        Atom.to_string(id) |> String.contains?("data_pipeline_test")
      end)
    end

    test "creates database records for single-node workflows during register_all" do
      # Clear any existing records
      Repo.delete_all(JobDefinition)

      # Run registration
      WorkflowRegistry.register_all()

      # Check that single-node workflows were stored as job definitions
      job_definitions = Repo.all(JobDefinition)

      # Should have at least one job definition (for our simple test job)
      assert length(job_definitions) > 0

      # Find our simple test job definition
      simple_job_def = Enum.find(job_definitions, fn jd ->
        String.contains?(jd.job_id, "simple_test_job")
      end)

      assert simple_job_def != nil
      assert simple_job_def.active == true
      assert simple_job_def.module == "ETL.Test.SimpleTestJob"
    end
  end

  describe "Workflow Information and Listing" do
    test "list_workflows returns comprehensive workflow information" do
      # Ensure workflows are discovered
      workflows_list = WorkflowRegistry.list_workflows()

      # Should include our test workflows
      simple_job = Enum.find(workflows_list, fn w ->
        Atom.to_string(w.workflow_id) |> String.contains?("simple_test_job")
      end)

      dag_workflow = Enum.find(workflows_list, fn w ->
        Atom.to_string(w.workflow_id) |> String.contains?("data_pipeline_test")
      end)

      # Verify simple job information
      if simple_job do
        assert simple_job.type == :job
        assert simple_job.node_count == 1
        assert simple_job.name == "Simple Test Job"
      end

      # Verify DAG information
      if dag_workflow do
        assert dag_workflow.type == :dag
        assert dag_workflow.node_count == 5
        assert dag_workflow.name == "Test Data Pipeline"
      end
    end

    test "get_workflow_info returns detailed configuration" do
      workflows = WorkflowRegistry.discover_workflows()

      if workflow = Enum.find(workflows, fn {id, _} ->
        Atom.to_string(id) |> String.contains?("simple_test_job")
      end) do
        {workflow_id, _} = workflow

        case WorkflowRegistry.get_workflow_info(workflow_id) do
          {:ok, config} ->
            assert config.name == "Simple Test Job"
            assert config.enable_progress == true
            assert Map.has_key?(config.nodes, :main)
          {:error, reason} ->
            flunk("Should have found workflow info: #{inspect(reason)}")
        end
      end
    end
  end

  ## Helper function for tests

  defp register_workflow_complete(workflow_id, workflow_config) do
    # This is a test helper that mimics the private function in WorkflowRegistry
    case WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config) do
      :ok ->
        case Orchestrator.register_workflow(workflow_id, workflow_config) do
          :ok ->
            if is_single_node_workflow?(workflow_config) do
              register_as_job_definition(workflow_id, workflow_config)
            end
            :ok
          error ->
            error
        end
      error ->
        error
    end
  end

  defp is_single_node_workflow?(workflow_config) do
    nodes = workflow_config[:nodes] || %{}
    map_size(nodes) == 1 and Map.has_key?(nodes, :main)
  end

  defp register_as_job_definition(workflow_id, workflow_config) do
    main_node = workflow_config.nodes[:main]

    job_config = %{
      module: main_node[:module],
      function: main_node[:function] || :run,
      args: main_node[:args] || [],
      schedule: workflow_config[:schedule] || :manual,
      enable_progress: workflow_config[:enable_progress] || false,
      max_retries: workflow_config[:max_retries] || 3,
      timeout: workflow_config[:timeout] || :infinity
    }

    attrs = Balsam.Schema.JobDefinition.from_config(workflow_id, job_config)

    %Balsam.Schema.JobDefinition{}
    |> Balsam.Schema.JobDefinition.changeset(attrs)
    |> Repo.insert(on_conflict: :replace_all, conflict_target: :job_id)

    :ok
  end
end
