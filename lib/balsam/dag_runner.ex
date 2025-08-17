defmodule Balsam.DagRunner do
  @moduledoc """
  Fault-tolerant DAG execution that handles node failures gracefully.
  Individual node failures don't crash the entire DAG unless they're critical path.
  """

  require Logger
  alias Balsam.{Repo, Schema.DagRun, JobRunner}
  use GenServer

  defstruct [
    :dag_id,
    :dag_run_id,
    :workflow_config,
    :completed_nodes,
    :running_nodes,
    :failed_nodes,
    :node_results,
    :retry_counts,
    :status
  ]

  ## Client API

  def start_dag(dag_id, workflow_config, run_id \\ nil) do
    run_id = run_id || generate_run_id(dag_id)

    case DynamicSupervisor.start_child(
      Balsam.DagSupervisor,
      {__MODULE__, {dag_id, workflow_config, run_id}}
    ) do
      {:ok, pid} ->
        Process.monitor(pid)
        Logger.info("Started DAG #{inspect(dag_id)} with run_id #{inspect(run_id)} in supervised process")
        {:ok, run_id}

      error ->
        Logger.error("Failed to start DAG #{inspect(dag_id)}: #{inspect(error)}")
        error
    end
  end

  def cancel_dag(dag_run_id) do
    # Find the DAG process and gracefully stop it
    case find_dag_process(dag_run_id) do
      {:ok, pid} ->
        GenServer.call(pid, :cancel)
      :not_found ->
        {:error, :dag_not_found}
    end
  end

  def get_dag_status(dag_id) do
    case get_latest_dag_run(dag_id) do
      nil ->
        {:ok, %{status: :never_run}}
      dag_run ->
        {:ok, format_dag_status(dag_run)}
    end
  end

  ## GenServer Implementation

  def start_link({dag_id, workflow_config, run_id}) do
    GenServer.start_link(__MODULE__, {dag_id, workflow_config, run_id})
  end

  @impl true
  def init({dag_id, workflow_config, run_id}) do
    case create_dag_run(dag_id, run_id) do
      {:ok, dag_run} ->
        state = %__MODULE__{
          dag_id: dag_id,
          dag_run_id: dag_run.id,
          workflow_config: workflow_config,
          completed_nodes: MapSet.new(),
          running_nodes: %{},
          failed_nodes: MapSet.new(),
          node_results: %{},
          retry_counts: %{},
          status: :running
        }

        # Start executing immediately
        send(self(), :execute_dag)
        {:ok, state}

      error ->
        Logger.error("Failed to initialize DAG #{dag_id}: #{inspect(error)}")
        {:stop, error}
    end
  end

  @impl true
  def handle_call(:cancel, _from, state) do
    Logger.info("Cancelling DAG #{state.dag_id}")

    # Cancel all running nodes
    Enum.each(state.running_nodes, fn {_node_id, %{task: task}} ->
      Task.shutdown(task, :brutal_kill)
    end)

    # Update database
    update_dag_run(state.dag_run_id, :cancelled, state.node_results)

    # Notify orchestrator
    notify_orchestrator(state.dag_run_id, {:dag_cancelled, self()})

    {:stop, :normal, :ok, %{state | status: :cancelled}}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      dag_id: state.dag_id,
      status: state.status,
      completed_nodes: MapSet.to_list(state.completed_nodes),
      running_nodes: Map.keys(state.running_nodes),
      failed_nodes: MapSet.to_list(state.failed_nodes),
      total_nodes: map_size(state.workflow_config.nodes),
      retry_counts: state.retry_counts
    }
    {:reply, {:ok, status}, state}
  end

  @impl true
  def handle_info(:execute_dag, state) do
    cond do
      dag_complete?(state) ->
        complete_dag(state)
        notify_orchestrator(state.dag_run_id, {:dag_completed, self()})
        {:stop, :normal, %{state | status: :completed}}

      dag_failed?(state) ->
        fail_dag(state)
        notify_orchestrator(state.dag_run_id, {:dag_failed, self(), "DAG failed"})
        {:stop, :normal, %{state | status: :failed}}

      true ->
        new_state = start_ready_nodes(state)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({ref, {:ok, result}}, state) when is_reference(ref) do
    case find_node_by_task_ref(state.running_nodes, ref) do
      {node_id, _node_info} ->
        Logger.info("Node #{inspect(node_id)} completed successfully")

        new_state = state
        |> add_completed_node(node_id, result)
        |> remove_running_node(node_id)

        # Process cleanup
        Process.demonitor(ref, [:flush])

        # Continue DAG execution
        send(self(), :execute_dag)
        {:noreply, new_state}

      nil ->
        Logger.warning("Received completion for unknown task ref #{inspect(ref)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({ref, {:error, reason}}, state) when is_reference(ref) do
    case find_node_by_task_ref(state.running_nodes, ref) do
      {node_id, _node_info} ->
        Logger.error("Node #{inspect(node_id)} failed: #{inspect(reason)}")

        new_state = handle_node_failure(state, node_id, reason)

        # Process cleanup
        Process.demonitor(ref, [:flush])

        # Continue DAG execution (might retry or proceed)
        send(self(), :execute_dag)
        {:noreply, new_state}

      nil ->
        Logger.warning("Received failure for unknown task ref #{inspect(ref)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case find_node_by_task_ref(state.running_nodes, ref) do
      {node_id, _node_info} ->
        Logger.error("Node #{inspect(node_id)} process died: #{inspect(reason)}")
        new_state = handle_node_failure(state, node_id, {:process_died, reason})
        send(self(), :execute_dag)
        {:noreply, new_state}

      nil ->
        Logger.warning("Process #{inspect(ref)} died but not found in running nodes")
        {:noreply, state}
    end
  end

  ## Private Functions

  defp dag_complete?(state) do
    total_nodes = map_size(state.workflow_config.nodes)
    completed_count = MapSet.size(state.completed_nodes)
    running_count = map_size(state.running_nodes)

    completed_count + running_count == total_nodes and running_count == 0
  end

  defp dag_failed?(state) do
    # DAG fails if:
    # 1. No ready nodes to start
    # 2. No running nodes
    # 3. Some nodes have failed and can't be retried
    ready_nodes = get_ready_nodes(state)
    running_count = map_size(state.running_nodes)

    ready_nodes == [] and running_count == 0 and MapSet.size(state.failed_nodes) > 0
  end

  defp start_ready_nodes(state) do
    ready_nodes = get_ready_nodes(state)
    max_concurrent = state.workflow_config[:max_concurrent_nodes] || 3
    current_running = map_size(state.running_nodes)
    available_slots = max(0, max_concurrent - current_running)

    nodes_to_start = Enum.take(ready_nodes, available_slots)

    Enum.reduce(nodes_to_start, state, fn node_id, acc ->
      case start_node(node_id, acc) do
        {:ok, task_ref} ->
          node_info = %{
            task: task_ref,
            started_at: System.system_time(:millisecond)
          }
          put_in(acc.running_nodes[node_id], node_info)

        {:error, reason} ->
          Logger.error("Failed to start node #{inspect(node_id)}: #{inspect(reason)}")
          handle_node_failure(acc, node_id, reason)
      end
    end)
  end

  defp get_ready_nodes(state) do
    state.workflow_config.nodes
    |> Enum.filter(fn {node_id, node_config} ->
      not MapSet.member?(state.completed_nodes, node_id) and
      not Map.has_key?(state.running_nodes, node_id) and
      not MapSet.member?(state.failed_nodes, node_id) and
      dependencies_satisfied?(node_config, state.completed_nodes)
    end)
    |> Enum.map(fn {node_id, _} -> node_id end)
  end

  defp dependencies_satisfied?(node_config, completed_nodes) do
    depends_on = node_config[:depends_on] || []
    Enum.all?(depends_on, &MapSet.member?(completed_nodes, &1))
  end

  defp start_node(node_id, state) do
    node_config = Map.get(state.workflow_config.nodes, node_id)

    # Start the node execution in a supervised task
    try do
      task = Task.async(fn ->
        execute_node_safely(node_id, node_config, state)
      end)

      {:ok, task.ref}
    rescue
      error ->
        {:error, error}
    end
  end

  defp execute_node_safely(node_id, node_config, state) do
    try do
      module = node_config[:module]
      function = node_config[:function]
      args = node_config[:args] || []

      # Create a simple progress callback for the node
      progress_callback = fn current, total, message ->
        Logger.debug("Node #{inspect(node_id)} progress: #{current}/#{total} - #{message}")
      end

      # Execute the node function
      result = case length(args) do
        0 ->
          if function_exported?(module, function, 1) do
            apply(module, function, [progress_callback])
          else
            apply(module, function, [])
          end
        _ ->
          apply(module, function, args ++ [progress_callback])
      end

      {:ok, result}

    rescue
      error ->
        Logger.error("Node #{inspect(node_id)} execution failed: #{inspect(error)}")
        {:error, error}
    catch
      :exit, reason ->
        {:error, {:exit, reason}}
      :throw, value ->
        {:error, {:throw, value}}
      kind, reason ->
        {:error, {kind, reason}}
    end
  end

  defp handle_node_failure(state, node_id, reason) do
    current_retry_count = Map.get(state.retry_counts, node_id, 0)
    max_retries = state.workflow_config[:max_retries] || 3

    if current_retry_count < max_retries do
      Logger.info("Will retry node #{inspect(node_id)} (attempt #{current_retry_count + 1}/#{max_retries})")

      # Increment retry count but don't mark as failed yet
      new_retry_counts = Map.put(state.retry_counts, node_id, current_retry_count + 1)
      %{state | retry_counts: new_retry_counts}
      |> remove_running_node(node_id)
    else
      Logger.error("Node #{inspect(node_id)} exceeded max retries, marking as failed")

      # Mark as permanently failed
      %{state | failed_nodes: MapSet.put(state.failed_nodes, node_id)}
      |> remove_running_node(node_id)
    end
  end

  defp add_completed_node(state, node_id, result) do
    %{state |
      completed_nodes: MapSet.put(state.completed_nodes, node_id),
      node_results: Map.put(state.node_results, node_id, result)
    }
  end

  defp remove_running_node(state, node_id) do
    %{state | running_nodes: Map.delete(state.running_nodes, node_id)}
  end

  defp find_node_by_task_ref(running_nodes, ref) do
    Enum.find(running_nodes, fn {_node_id, %{task: task}} ->
      task == ref
    end)
  end

  defp complete_dag(state) do
    # Flatten the results to remove tuples
    flattened_results = Map.new(state.node_results, fn {node_id, result} ->
      case result do
        {:ok, data} -> {node_id, %{status: "success", data: data}}
        {:error, error} -> {node_id, %{status: "error", error: inspect(error)}}
        other -> {node_id, %{status: "unknown", data: inspect(other)}}
      end
    end)

    Logger.info("DAG #{state.dag_id} completed successfully")
    update_dag_run(state.dag_run_id, :completed, flattened_results)
  end

  defp fail_dag(state) do
    Logger.error("DAG #{state.dag_id} failed")
    failure_info = %{
      failed_nodes: MapSet.to_list(state.failed_nodes),
      completed_nodes: MapSet.to_list(state.completed_nodes),
      retry_counts: state.retry_counts
    }
    update_dag_run(state.dag_run_id, :failed, failure_info)
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

  defp get_latest_dag_run(dag_id) do
    # This would be implemented with proper database queries
    # For now, return nil to avoid compilation issues
    nil
  end

  defp format_dag_status(dag_run) do
    %{
      status: dag_run.status,
      run_id: dag_run.run_id,
      started_at: dag_run.started_at,
      completed_at: dag_run.completed_at
    }
  end

  defp generate_run_id(dag_id) do
    timestamp = System.system_time(:millisecond)
    "#{dag_id}_#{timestamp}"
  end

  defp notify_orchestrator(dag_run_id, message) do
    if orchestrator_pid = Process.whereis(Balsam.Orchestrator) do
      send(orchestrator_pid, {message, dag_run_id})
    end
  end

  defp find_dag_process(_dag_run_id) do
    # In a real implementation, maintain a registry of DAG processes
    :not_found
  end
end
