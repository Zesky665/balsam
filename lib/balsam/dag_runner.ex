defmodule Balsam.DagRunner do
  @moduledoc """
  Handles execution of multi-step DAG workflows.
  """

  require Logger
  alias Balsam.{Repo, Schema.DagRun, JobRunner}
  use GenServer

  @doc """
  Start a DAG execution.
  """
  def start_dag(dag_id, workflow_config, run_id \\ nil) do
    run_id = run_id || generate_run_id(dag_id)

    case create_dag_run(dag_id, run_id) do
      {:ok, dag_run} ->
        {:ok, pid} = GenServer.start_link(__MODULE__, {dag_id, workflow_config, dag_run.id})
        Process.monitor(pid)
        Logger.info("Started DAG #{dag_id} with run_id #{run_id}")
        {:ok, run_id}

      error ->
        Logger.error("Failed to start DAG #{dag_id}: #{inspect(error)}")
        error
    end
  end

  @doc """
  Cancel a running DAG.
  """
  def cancel_dag(_dag_run_id) do
    # Find the GenServer process and stop it
    Logger.info("Cancelling DAG run")
    :ok
  end

  @doc """
  Get DAG status.
  """
  def get_dag_status(dag_id) do
    # Query database for latest DAG run status
    case get_latest_dag_run(dag_id) do
      nil ->
        {:ok, %{status: :never_run}}
      dag_run when is_map(dag_run) ->
        {:ok, %{
          status: Map.get(dag_run, :status),
          run_id: Map.get(dag_run, :run_id),
          started_at: Map.get(dag_run, :started_at),
          completed_at: Map.get(dag_run, :completed_at)
        }}
      _other ->
        {:ok, %{status: :unknown}}
    end
  end

  ## GenServer Implementation for DAG Execution

  @impl true
  def init({dag_id, workflow_config, dag_run_id}) do
    state = %{
      dag_id: dag_id,
      dag_run_id: dag_run_id,
      workflow_config: workflow_config,
      completed_nodes: [],
      running_nodes: %{},
      failed_nodes: [],
      node_results: %{}
    }

    # Start executing immediately
    GenServer.cast(self(), :execute_dag)

    {:ok, state}
  end

  @impl true
  def handle_cast(:execute_dag, state) do
    cond do
      dag_complete?(state) ->
        complete_dag(state)
        send_completion_message(state.dag_run_id)
        {:stop, :normal, state}

      dag_failed?(state) ->
        fail_dag(state)
        send_failure_message(state.dag_run_id, "DAG failed")
        {:stop, :normal, state}

      true ->
        new_state = start_ready_nodes(state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:node_completed, node_id, result}, state) do
    Logger.info("Node #{node_id} completed")

    new_state = state
    |> Map.update!(:completed_nodes, &[node_id | &1])
    |> Map.update!(:running_nodes, &Map.delete(&1, node_id))
    |> Map.update!(:node_results, &Map.put(&1, node_id, result))

    GenServer.cast(self(), :execute_dag)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:node_failed, node_id, error}, state) do
    Logger.error("Node #{node_id} failed: #{inspect(error)}")

    new_state = state
    |> Map.update!(:failed_nodes, &[node_id | &1])
    |> Map.update!(:running_nodes, &Map.delete(&1, node_id))

    GenServer.cast(self(), :execute_dag)
    {:noreply, new_state}
  end

  ## Private Functions

  defp dag_complete?(state) do
    total_nodes = map_size(state.workflow_config.nodes)
    completed_count = length(state.completed_nodes)
    running_count = map_size(state.running_nodes)

    completed_count + running_count == total_nodes and running_count == 0
  end

  defp dag_failed?(state) do
    length(state.failed_nodes) > 0 and
      get_ready_nodes(state) == [] and
      map_size(state.running_nodes) == 0
  end

  defp start_ready_nodes(state) do
    ready_nodes = get_ready_nodes(state)
    max_concurrent = state.workflow_config[:max_concurrent_nodes] || 3
    current_running = map_size(state.running_nodes)
    available_slots = max(0, max_concurrent - current_running)

    nodes_to_start = Enum.take(ready_nodes, available_slots)

    Enum.reduce(nodes_to_start, state, fn node_id, acc ->
      case start_node(node_id, acc) do
        {:ok, job_ref} ->
          Map.update!(acc, :running_nodes, &Map.put(&1, node_id, job_ref))
        {:error, reason} ->
          Logger.error("Failed to start node #{node_id}: #{inspect(reason)}")
          Map.update!(acc, :failed_nodes, &[node_id | &1])
      end
    end)
  end

  defp get_ready_nodes(state) do
    completed_set = MapSet.new(state.completed_nodes)
    running_set = MapSet.new(Map.keys(state.running_nodes))
    failed_set = MapSet.new(state.failed_nodes)

    state.workflow_config.nodes
    |> Enum.filter(fn {node_id, node_config} ->
      not MapSet.member?(completed_set, node_id) and
      not MapSet.member?(running_set, node_id) and
      not MapSet.member?(failed_set, node_id) and
      dependencies_satisfied?(node_config, completed_set)
    end)
    |> Enum.map(fn {node_id, _} -> node_id end)
  end

  defp dependencies_satisfied?(node_config, completed_set) do
    depends_on = node_config[:depends_on] || []
    Enum.all?(depends_on, &MapSet.member?(completed_set, &1))
  end

  defp start_node(node_id, state) do
    node_config = Map.get(state.workflow_config.nodes, node_id)

    # Create a single-node workflow config for this node
    single_node_config = %{
      nodes: %{
        main: %{
          module: node_config[:module],
          function: node_config[:function],
          args: node_config[:args] || []
        }
      }
    }

    # Start as a job and monitor it
    case JobRunner.start_job(node_id, single_node_config) do
      {:ok, job_ref} ->
        spawn_link(fn -> monitor_node(node_id, job_ref, self()) end)
        {:ok, job_ref}
      error ->
        error
    end
  end

  defp monitor_node(node_id, job_ref, dag_pid) do
    # Simple monitoring - in practice you'd integrate with JobRunner's monitoring
    receive do
      {:job_completed, ^job_ref} ->
        send(dag_pid, {:node_completed, node_id, :success})
      {:job_failed, ^job_ref, error} ->
        send(dag_pid, {:node_failed, node_id, error})
    after
      :timer.minutes(30) ->
        send(dag_pid, {:node_failed, node_id, :timeout})
    end
  end

  defp complete_dag(state) do
    update_dag_run(state.dag_run_id, :completed, state.node_results)
  end

  defp fail_dag(state) do
    update_dag_run(state.dag_run_id, :failed, %{failed_nodes: state.failed_nodes})
  end

  defp create_dag_run(dag_id, run_id) do
    attrs = %{
      dag_id: to_string(dag_id),
      run_id: run_id,
      started_at: DateTime.utc_now(),
      metadata: "{}"
    }

    %DagRun{}
    |> DagRun.start_changeset(attrs)
    |> Repo.insert()
  end

  defp update_dag_run(dag_run_id, status, results) do
    try do
      dag_run = Repo.get!(DagRun, dag_run_id)

      attrs = %{
        status: status,
        completed_at: DateTime.utc_now(),
        node_results: results
      }

      dag_run
      |> DagRun.complete_changeset(attrs)
      |> Repo.update()
    rescue
      error ->
        Logger.error("Failed to update DAG run #{dag_run_id}: #{inspect(error)}")
    end
  end

  defp get_latest_dag_run(_dag_id) do
    # Query implementation would go here
    # For now, return nil to avoid undefined function warnings
    nil
  end

  defp generate_run_id(dag_id) do
    timestamp = System.system_time(:millisecond)
    "#{dag_id}_#{timestamp}"
  end

  defp send_completion_message(dag_run_id) do
    if Process.whereis(Balsam.Orchestrator) do
      send(Process.whereis(Balsam.Orchestrator), {:dag_completed, dag_run_id})
    end
  end

  defp send_failure_message(dag_run_id, reason) do
    if Process.whereis(Balsam.Orchestrator) do
      send(Process.whereis(Balsam.Orchestrator), {:dag_failed, dag_run_id, reason})
    end
  end
end
