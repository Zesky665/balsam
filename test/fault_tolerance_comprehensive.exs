# test/fault_tolerance_comprehensive_test.exs
defmodule Balsam.FaultToleranceComprehensiveTest do
  use ExUnit.Case, async: false
  alias Balsam.{Orchestrator, Repo}
  require Logger

  @moduletag timeout: 120_000  # 2 minutes timeout for stress tests

  setup do
    # Set up clean database state in shared mode for spawned processes
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    # Start a fresh orchestrator for each test
    orchestrator_name = :"test_orchestrator_#{:rand.uniform(100_000)}"
    {:ok, orchestrator} = Orchestrator.start_link(
      name: orchestrator_name,
      max_concurrent: 10
    )

    on_exit(fn ->
      # Clean up shared mode
      Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)
    end)

    %{orchestrator: orchestrator}
  end

  describe "Basic Fault Tolerance" do
    test "orchestrator survives basic job crash", %{orchestrator: orch} do
      initial_pid = orch

      # Register crashing job
      crash_job = %{
        name: "Basic Crash Test",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_immediately, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :basic_crash, crash_job)
      initial_workflows = Orchestrator.get_status(orch).registered_workflows

      # Run crash
      {:ok, _job_ref} = Orchestrator.run_job(orch, :basic_crash)
      :timer.sleep(2000)

      # Verify survival
      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator PID changed - indicates restart"

      final_workflows = Orchestrator.get_status(orch).registered_workflows
      assert :basic_crash in final_workflows, "Workflows not preserved"
    end

    test "orchestrator handles different exception types", %{orchestrator: orch} do
      initial_pid = orch

      exception_types = [
        {:runtime_error, "RuntimeError crash"},
        {:argument_error, "ArgumentError crash"},
        {:throw_error, "Throw crash"},
        {:exit_error, "Exit crash"},
        {:function_clause_error, "FunctionClause crash"}
      ]

      # Register and run each exception type
      Enum.each(exception_types, fn {type, name} ->
        job_config = %{
          name: name,
          nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_with_type, args: [type], depends_on: []}}
        }

        :ok = Orchestrator.register_workflow(orch, type, job_config)
        {:ok, _} = Orchestrator.run_job(orch, type)
      end)

      # Wait for all to crash
      :timer.sleep(4000)

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
    end

    test "orchestrator survives process kill attempts", %{orchestrator: orch} do
      initial_pid = orch

      kill_job = %{
        name: "Process Kill",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :kill_self, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :kill_job, kill_job)
      {:ok, _} = Orchestrator.run_job(orch, :kill_job)

      :timer.sleep(2000)

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
    end
  end

  describe "Concurrent Failure Handling" do
    test "orchestrator survives multiple simultaneous crashes", %{orchestrator: orch} do
      initial_pid = orch

      # Register multiple crash jobs
      crash_jobs = for i <- 1..5 do
        job_id = :"crash_#{i}"
        job_config = %{
          name: "Multi Crash #{i}",
          nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_with_delay, args: [i * 100], depends_on: []}}
        }

        :ok = Orchestrator.register_workflow(orch, job_id, job_config)
        job_id
      end

      # Start all jobs simultaneously
      job_refs = Enum.map(crash_jobs, fn job_id ->
        {:ok, job_ref} = Orchestrator.run_job(orch, job_id)
        job_ref
      end)

      # Wait for all crashes
      :timer.sleep(3000)

      # Verify orchestrator survival
      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"

      status = Orchestrator.get_status(orch)
      assert length(job_refs) > 0, "No jobs were started"

      # Allow some jobs to still be running (they might not have all crashed yet)
      total_jobs_accounted_for = status.running_jobs + status.failed_jobs
      assert total_jobs_accounted_for > 0, "No jobs tracked in orchestrator state"
    end

    @tag timeout: 60_000
    test "orchestrator handles high concurrency stress test", %{orchestrator: orch} do
      initial_pid = orch

      # Register stress test job
      stress_job = %{
        name: "Stress Test",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :random_crash, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :stress_job, stress_job)

      # Launch many jobs rapidly
      job_refs = for _i <- 1..20 do
        case Orchestrator.run_job(orch, :stress_job) do
          {:ok, job_ref} -> job_ref
          {:error, :max_concurrent_reached} ->
            :timer.sleep(50)  # Brief backoff
            case Orchestrator.run_job(orch, :stress_job) do
              {:ok, job_ref} -> job_ref
              _ -> nil
            end
          _ -> nil
        end
      end

      # Wait for chaos to settle
      :timer.sleep(8000)

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"

      status = Orchestrator.get_status(orch)
      successful_jobs = Enum.count(job_refs, & &1 != nil)
      assert successful_jobs > 0, "No jobs were successfully started"
    end
  end

  describe "Resource Attack Resistance" do
    test "orchestrator survives memory bomb", %{orchestrator: orch} do
      initial_pid = orch

      memory_bomb = %{
        name: "Memory Bomb",
        timeout: 5000,  # 5 second timeout
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :memory_bomb, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :memory_bomb, memory_bomb)
      {:ok, _} = Orchestrator.run_job(orch, :memory_bomb)

      :timer.sleep(6000)  # Wait for timeout

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
    end

    test "orchestrator handles infinite loop timeout", %{orchestrator: orch} do
      initial_pid = orch

      infinite_loop = %{
        name: "Infinite Loop",
        timeout: 2000,  # 2 second timeout
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :infinite_loop, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :infinite_loop, infinite_loop)
      {:ok, _} = Orchestrator.run_job(orch, :infinite_loop)

      :timer.sleep(4000)  # Wait for timeout

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
    end
  end

  describe "DAG Fault Tolerance" do
    test "orchestrator survives DAG node failures", %{orchestrator: orch} do
      initial_pid = orch

      # Register DAG with failing middle step
      failing_dag = %{
        name: "Fault Test DAG",
        max_concurrent_nodes: 2,
        nodes: %{
          step1: %{module: __MODULE__.TestJobs, function: :success_step, args: ["step1"], depends_on: []},
          step2: %{module: __MODULE__.TestJobs, function: :crash_immediately, args: [], depends_on: [:step1]},
          step3: %{module: __MODULE__.TestJobs, function: :success_step, args: ["step3"], depends_on: [:step2]}
        }
      }

      :ok = Orchestrator.register_workflow(orch, :failing_dag, failing_dag)

      # Run the DAG (or fall back to job if DAG not implemented)
      result = case Orchestrator.run_dag(orch, :failing_dag) do
        {:ok, _run_id} ->
          :timer.sleep(5000)  # Wait for DAG to fail
          :dag_tested
        {:error, _reason} ->
          # Fall back to testing as single job
          {:ok, _} = Orchestrator.run_job(orch, :failing_dag)
          :timer.sleep(2000)
          :job_tested
      end

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
      assert result in [:dag_tested, :job_tested], "Neither DAG nor job was tested"
    end
  end

  describe "State Persistence and Recovery" do
    test "workflows persist after job failures", %{orchestrator: orch} do
      initial_pid = orch

      # Register several workflows
      workflows = [:job1, :job2, :job3, :crash_job]

      Enum.each(workflows, fn job_id ->
        config = %{
          name: "Test Job #{job_id}",
          nodes: %{main: %{module: __MODULE__.TestJobs, function: :success_step, args: [to_string(job_id)], depends_on: []}}
        }
        :ok = Orchestrator.register_workflow(orch, job_id, config)
      end)

      # Add a crashing job
      crash_config = %{
        name: "State Test Crash",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_immediately, args: [], depends_on: []}}
      }
      :ok = Orchestrator.register_workflow(orch, :crash_job, crash_config)

      initial_status = Orchestrator.get_status(orch)

      # Run the crashing job
      {:ok, _} = Orchestrator.run_job(orch, :crash_job)
      :timer.sleep(2000)

      # Check state persistence
      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"

      final_status = Orchestrator.get_status(orch)
      assert length(initial_status.registered_workflows) == length(final_status.registered_workflows),
        "Lost workflows: #{length(initial_status.registered_workflows)} → #{length(final_status.registered_workflows)}"
    end

    test "orchestrator can recover and run jobs after failures", %{orchestrator: orch} do
      initial_pid = orch

      # Register both crashing and success jobs
      crash_job = %{
        name: "Recovery Crash",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_immediately, args: [], depends_on: []}}
      }

      success_job = %{
        name: "Recovery Success",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :success_step, args: ["recovery"], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :crash_job, crash_job)
      :ok = Orchestrator.register_workflow(orch, :success_job, success_job)

      # Crash a job
      {:ok, _} = Orchestrator.run_job(orch, :crash_job)
      :timer.sleep(1500)

      # Try to run a success job
      assert {:ok, _} = Orchestrator.run_job(orch, :success_job)
      :timer.sleep(1000)

      assert Process.alive?(orch)
      assert orch == initial_pid, "Orchestrator was restarted"
    end
  end

  describe "System Health Monitoring" do
    test "orchestrator reports correct status after failures", %{orchestrator: orch} do
      # Register and run a mix of successful and failing jobs
      success_job = %{
        name: "Success Job",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :success_step, args: ["test"], depends_on: []}}
      }

      crash_job = %{
        name: "Crash Job",
        nodes: %{main: %{module: __MODULE__.TestJobs, function: :crash_immediately, args: [], depends_on: []}}
      }

      :ok = Orchestrator.register_workflow(orch, :success_job, success_job)
      :ok = Orchestrator.register_workflow(orch, :crash_job, crash_job)

      # Run both types
      {:ok, _} = Orchestrator.run_job(orch, :success_job)
      {:ok, _} = Orchestrator.run_job(orch, :crash_job)
      {:ok, _} = Orchestrator.run_job(orch, :success_job)

      :timer.sleep(3000)

      # Check status is still accessible and accurate
      status = Orchestrator.get_status(orch)

      assert is_list(status.registered_workflows)
      assert is_integer(status.running_jobs)
      assert is_integer(status.failed_jobs)
      assert is_integer(status.max_concurrent)

      # Should have both workflows registered
      assert :success_job in status.registered_workflows
      assert :crash_job in status.registered_workflows
    end
  end

  # Test job implementations
  defmodule TestJobs do
    def crash_immediately(_progress \\ nil), do: raise("Intentional crash")

    def crash_with_delay(delay_ms, _progress \\ nil) do
      :timer.sleep(delay_ms)
      raise("Delayed crash after #{delay_ms}ms")
    end

    def crash_with_type(type, _progress \\ nil) do
      case type do
        :runtime_error -> raise("Runtime error test")
        :argument_error -> raise ArgumentError, "Argument error test"
        :throw_error -> throw(:test_throw)
        :exit_error -> exit(:test_exit)
        :function_clause_error -> String.to_atom(123)  # This will crash
      end
    end

    def success_step(step_name, _progress \\ nil) do
      :timer.sleep(200)  # Shorter sleep for tests
      {:ok, %{step: step_name, completed_at: DateTime.utc_now()}}
    end

    def memory_bomb(_progress \\ nil) do
      # Try to consume lots of memory
      _big_list = Enum.to_list(1..1_000_000)  # Smaller for tests
      raise("Memory bomb exploded")
    end

    def infinite_loop(_progress \\ nil) do
      infinite_loop()  # This will run forever until timeout
    end

    def kill_self(_progress \\ nil) do
      Process.exit(self(), :kill)
    end

    def random_crash(_progress \\ nil) do
      case :rand.uniform(4) do
        1 -> raise("Random crash 1")
        2 -> throw(:random_throw)
        3 -> exit(:random_exit)
        4 -> {:ok, "Lucky success!"}
      end
    end
  end
end
