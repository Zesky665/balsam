defmodule Balsam.Orchestrator do
  use GenServer
  require Logger
  alias Balsam.{Orchestrator, Repo, Schema.DagDefinition, Schema.DagNode, Schema.DagRun}
  import Ecto.Query

  @moduledoc """
  DAG orchestrator that coordinates execution of job dependencies.

  Features:
  - Dependency resolution and parallel execution
  - Integration with existing Balsam.Orchestrator for job execution
  - Database persistence for DAG runs and node statuses
  - Conditional node execution
  - Retry logic at both DAG and node level
  """

  defstruct [
    :dags,
    :running_dag_runs,
    :orchestrator_pid,
    :schedule_timer,
    :max_concurrent_dags
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Register a new DAG with the orchestrator.

  ## Options

    * `:name` - Human-readable name for the DAG
    * `:description` - Description of what this DAG does
    * `:nodes` - Map of node configurations
    * `:schedule` - When to run: :manual or {:interval, milliseconds}
    * `:max_concurrent_nodes` - Maximum nodes to run simultaneously (default: 3)

  ## Example

      nodes = %{
        extract: %{job_id: :data_extractor, depends_on: []},
        transform: %{job_id: :data_transformer, depends_on: [:extract]},
        load: %{job_id: :data_loader, depends_on: [:transform]}
      }

      Balsam.DagOrchestrator.register_dag(:etl_pipeline, %{
        name: "Daily ETL Pipeline",
        description: "Extract, transform, and load daily data",
        nodes: nodes,
        schedule: {:interval, :timer.hours(24)},
        max_concurrent_nodes: 2
      })
  """
  def register_dag(orchestrator \\ __MODULE__, dag_id, dag_config) do
    GenServer.call(orchestrator, {:register_dag, dag_id, dag_config})
  end

  @doc """
  Run a DAG immediately.
  """
  def run_dag(orchestrator \\ __MODULE__, dag_id, run_id \\ nil) do
    GenServer.call(orchestrator, {:run_dag, dag_id, run_id}, :infinity)
  end

  @doc """
  Cancel a running DAG.
  """
  def cancel_dag(orchestrator \\ __MODULE__, dag_id) do
    GenServer.call(orchestrator, {:cancel_dag, dag_id})
  end

  @doc """
  Get status of a specific DAG.
  """
  def dag_status(orchestrator \\ __MODULE__, dag_id) do
    GenServer.call(orchestrator, {:dag_status, dag_id})
  end

  @doc """
  List all currently running DAGs.
  """
  def list_running_dags(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :list_running_dags)
  end

  @doc """
  Get overall DAG orchestrator status.
  """
  def get_status(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :get_status)
  end

  @doc """
  Get DAG execution history from database.
  """
  def get_dag_history(dag_id, opts \\ []) do
    GenServer.call(__MODULE__, {:get_dag_history, dag_id, opts})
  end

  def cancel_job(orchestrator \\ __MODULE__, job_id) do
    cancel_dag(orchestrator, job_id)
  end

  def run_job(orchestrator \\ __MODULE__, job_id) do
    run_dag(orchestrator, job_id)
  end


  ## Server Implementation

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      dags: %{},
      running_dag_runs: %{},
      orchestrator_pid: Keyword.get(opts, :orchestrator_pid, Orchestrator),
      schedule_timer: nil,
      max_concurrent_dags: Keyword.get(opts, :max_concurrent_dags, 2)
    }

    # Load persisted DAG definitions
    loaded_state = load_persisted_dags(state)

    Logger.info("DAG Orchestrator started with max #{loaded_state.max_concurrent_dags} concurrent DAGs")
    Logger.info("Loaded #{map_size(loaded_state.dags)} persisted DAG definitions")

    {:ok, schedule_next_check(loaded_state)}
  end

  @impl true
  def handle_call({:register_dag, dag_id, dag_config}, _from, state) do
    case validate_dag_config(dag_config) do
      {:ok, validated_config} ->
        case persist_dag_definition(dag_id, validated_config) do
          {:ok, _dag_def} ->
            updated_dags = Map.put(state.dags, dag_id, validated_config)
            Logger.info("Registered DAG: #{dag_id} with #{map_size(validated_config.nodes)} nodes")
            {:reply, :ok, %{state | dags: updated_dags}}

          {:error, reason} ->
            Logger.error("Failed to persist DAG #{dag_id}: #{inspect(reason)}")
            {:reply, {:error, :persistence_failed}, state}
        end

      {:error, reason} ->
        Logger.error("Failed to register DAG #{dag_id}: #{reason}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:run_dag, dag_id, run_id}, _from, state) do
    cond do
      not Map.has_key?(state.dags, dag_id) ->
        {:reply, {:error, :dag_not_found}, state}

      map_size(state.running_dag_runs) >= state.max_concurrent_dags ->
        {:reply, {:error, :max_concurrent_dags_reached}, state}

      true ->
        run_id = run_id || generate_run_id(dag_id)
        dag_config = Map.get(state.dags, dag_id)

        case start_dag_run(dag_id, run_id, dag_config) do
          {:ok, dag_run_info} ->
            new_running_dags = Map.put(state.running_dag_runs, run_id, dag_run_info)
            Logger.info("Started DAG run: #{run_id}")

            # Start the DAG execution process
            GenServer.cast(self(), {:execute_dag, run_id})

            {:reply, {:ok, run_id}, %{state | running_dag_runs: new_running_dags}}

          {:error, reason} = error ->
            Logger.error("Failed to start DAG #{dag_id}: #{inspect(reason)}")
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call({:cancel_dag, dag_id}, _from, state) do
    case find_running_dag_by_dag_id(state.running_dag_runs, dag_id) do
      nil ->
        {:reply, {:error, :dag_not_running}, state}

      {run_id, dag_run_info} ->
        # Cancel all running nodes
        Enum.each(dag_run_info.running_nodes, fn {node_id, job_ref} ->
          Orchestrator.cancel_job(state.orchestrator_pid, job_ref)
          Logger.info("Cancelled node #{node_id} in DAG run #{run_id}")
        end)

        # Mark DAG as cancelled in database
        complete_dag_run(dag_run_info.dag_run_id, :cancelled, %{
          cancelled_nodes: Map.keys(dag_run_info.running_nodes)
        })

        new_running_dags = Map.delete(state.running_dag_runs, run_id)
        Logger.info("Cancelled DAG run: #{run_id}")

        {:reply, :ok, %{state | running_dag_runs: new_running_dags}}
    end
  end

  @impl true
  def handle_call({:dag_status, dag_id}, _from, state) do
    case find_running_dag_by_dag_id(state.running_dag_runs, dag_id) do
      nil ->
        if Map.has_key?(state.dags, dag_id) do
          # Check last run from database
          case get_latest_dag_run(dag_id) do
            nil ->
              {:reply, {:ok, %{status: :never_run}}, state}

            last_run ->
              status = %{
                status: :not_running,
                last_run: %{
                  run_id: last_run.run_id,
                  status: last_run.status,
                  started_at: last_run.started_at,
                  completed_at: last_run.completed_at,
                  duration_ms: last_run.duration_ms,
                  node_statuses: last_run.node_statuses
                }
              }
              {:reply, {:ok, status}, state}
          end
        else
          {:reply, {:error, :dag_not_found}, state}
        end

      {run_id, dag_run_info} ->
        status = %{
          status: :running,
          run_id: run_id,
          started_at: dag_run_info.started_at,
          running_time_ms: System.system_time(:millisecond) - dag_run_info.started_at,
          completed_nodes: dag_run_info.completed_nodes,
          running_nodes: Map.keys(dag_run_info.running_nodes),
          failed_nodes: dag_run_info.failed_nodes
        }

        {:reply, {:ok, status}, state}
    end
  end

  @impl true
  def handle_call(:list_running_dags, _from, state) do
    running_dags = state.running_dag_runs
    |> Enum.map(fn {run_id, dag_run_info} ->
      {run_id, %{
        dag_id: dag_run_info.dag_id,
        started_at: dag_run_info.started_at,
        running_time_ms: System.system_time(:millisecond) - dag_run_info.started_at,
        completed_nodes: length(dag_run_info.completed_nodes),
        running_nodes: map_size(dag_run_info.running_nodes),
        failed_nodes: length(dag_run_info.failed_nodes)
      }}
    end)
    |> Enum.into(%{})

    {:reply, running_dags, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      registered_dags: Map.keys(state.dags),
      running_dag_count: map_size(state.running_dag_runs),
      max_concurrent: state.max_concurrent_dags,
      available_slots: state.max_concurrent_dags - map_size(state.running_dag_runs)
    }
    {:reply, status, state}
  end

  @impl true
  def handle_call({:get_dag_history, dag_id, opts}, _from, state) do
    history = list_dag_runs(dag_id, opts)
    {:reply, history, state}
  end

  ## Handle DAG execution
  ## Handle node completion
  @impl true
  def handle_info({:node_completed, run_id, node_id, result}, state) do
    case Map.get(state.running_dag_runs, run_id) do
      nil ->
        {:noreply, state}

      dag_run_info ->
        Logger.info("Node #{node_id} in DAG run #{run_id} completed successfully")

        # Update DAG run info
        updated_dag_run_info = dag_run_info
        |> Map.update!(:completed_nodes, &[node_id | &1])
        |> Map.update!(:running_nodes, &Map.delete(&1, node_id))
        |> Map.update!(:node_results, &Map.put(&1, node_id, result))

        new_running_dags = Map.put(state.running_dag_runs, run_id, updated_dag_run_info)
        new_state = %{state | running_dag_runs: new_running_dags}

        # Continue DAG execution
        GenServer.cast(self(), {:execute_dag, run_id})

        {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({:node_failed, run_id, node_id, error}, state) do
    case Map.get(state.running_dag_runs, run_id) do
      nil ->
        {:noreply, state}

      dag_run_info ->
        Logger.error("Node #{node_id} in DAG run #{run_id} failed: #{inspect(error)}")

        # Update DAG run info
        updated_dag_run_info = dag_run_info
        |> Map.update!(:failed_nodes, &[node_id | &1])
        |> Map.update!(:running_nodes, &Map.delete(&1, node_id))

        new_running_dags = Map.put(state.running_dag_runs, run_id, updated_dag_run_info)
        new_state = %{state | running_dag_runs: new_running_dags}

        # Continue DAG execution (may complete due to failure)
        GenServer.cast(self(), {:execute_dag, run_id})

        {:noreply, new_state}
    end
  end

  ## Handle scheduled DAG execution
  @impl true
  def handle_info(:check_scheduled_dags, state) do
    available_slots = state.max_concurrent_dags - map_size(state.running_dag_runs)

    if available_slots > 0 do
      try do
        due_dags = state.dags
        |> Enum.filter(fn {dag_id, dag_config} ->
          should_run_dag?(dag_config) and not is_dag_running?(state.running_dag_runs, dag_id)
        end)
        |> Enum.take(available_slots)

        Enum.each(due_dags, fn {dag_id, _config} ->
          GenServer.cast(self(), {:start_scheduled_dag, dag_id})
        end)
      rescue
        error ->
          Logger.error("Error in scheduled DAG check: #{inspect(error)}")
      end
    end

    {:noreply, schedule_next_check(state)}
  end

  @impl true
  def handle_cast({:execute_dag, run_id}, state) do
    case Map.get(state.running_dag_runs, run_id) do
      nil ->
        Logger.warning("Received execute_dag for unknown run_id: #{run_id}")
        {:noreply, state}

      dag_run_info ->
        new_state = execute_dag_step(state, run_id, dag_run_info)
        {:noreply, new_state}
    end
  end

  @impl true
  def handle_cast({:start_scheduled_dag, dag_id}, state) do
    case handle_call({:run_dag, dag_id, nil}, nil, state) do
      {:reply, {:ok, _run_id}, new_state} ->
        {:noreply, new_state}

      {:reply, error, new_state} ->
        Logger.warning("Failed to start scheduled DAG #{dag_id}: #{inspect(error)}")
        {:noreply, new_state}
    end
  end


  ## Private Functions

  defp validate_dag_config(config) when is_map(config) do
    required_fields = [:nodes]

    case Enum.find(required_fields, fn field -> not Map.has_key?(config, field) end) do
      nil ->
        case validate_dag_structure(config.nodes) do
          :ok ->
            defaults = %{
              name: "Unnamed DAG",
              description: "",
              schedule: :manual,
              last_run: nil,
              max_concurrent_nodes: 3,
              max_retries: 3,
              timeout: :infinity
            }

            validated_config = Map.merge(defaults, config)
            {:ok, validated_config}

          {:error, reason} ->
            {:error, reason}
        end

      missing_field ->
        {:error, "Missing required field: #{missing_field}"}
    end
  end
  defp validate_dag_config(_), do: {:error, "DAG config must be a map"}

  defp validate_dag_structure(nodes) when is_map(nodes) do
    # Check for cycles and validate dependencies
    node_ids = Map.keys(nodes) |> MapSet.new()

    # Validate all dependencies exist
    case Enum.find(nodes, fn {_node_id, node_config} ->
      depends_on = node_config[:depends_on] || []
      not Enum.all?(depends_on, &MapSet.member?(node_ids, &1))
    end) do
      {node_id, _config} ->
        {:error, "Node #{node_id} has invalid dependencies"}

      nil ->
        # Check for cycles using DFS
        case detect_cycles(nodes) do
          nil -> :ok
          cycle -> {:error, "Cycle detected: #{inspect(cycle)}"}
        end
    end
  end
  defp validate_dag_structure(_), do: {:error, "Nodes must be a map"}

  defp detect_cycles(nodes) do
    visited = MapSet.new()
    rec_stack = MapSet.new()

    Enum.find_value(Map.keys(nodes), fn node_id ->
      if not MapSet.member?(visited, node_id) do
        case dfs_cycle_check(node_id, nodes, visited, rec_stack, []) do
          {:cycle, path} -> path
          :no_cycle -> nil
        end
      end
    end)
  end

  defp dfs_cycle_check(node_id, nodes, visited, rec_stack, path) do
    if MapSet.member?(rec_stack, node_id) do
      {:cycle, Enum.reverse([node_id | path])}
    else
      if not MapSet.member?(visited, node_id) do
        visited = MapSet.put(visited, node_id)
        rec_stack = MapSet.put(rec_stack, node_id)
        new_path = [node_id | path]

        node_config = Map.get(nodes, node_id, %{})
        depends_on = node_config[:depends_on] || []

        case Enum.find_value(depends_on, fn dep ->
          case dfs_cycle_check(dep, nodes, visited, rec_stack, new_path) do
            {:cycle, cycle_path} -> {:cycle, cycle_path}
            :no_cycle -> nil
          end
        end) do
          {:cycle, cycle_path} -> {:cycle, cycle_path}
          nil -> :no_cycle
        end
      else
        :no_cycle
      end
    end
  end

  defp start_dag_run(dag_id, run_id, dag_config) do
    # Create DAG run record in database
    attrs = %{
      dag_id: to_string(dag_id),
      run_id: run_id,
      started_at: DateTime.utc_now(),
      metadata: inspect(%{config: dag_config})
    }

    case %DagRun{}
         |> DagRun.start_changeset(attrs)
         |> Repo.insert() do
      {:ok, dag_run} ->
        dag_run_info = %{
          dag_id: dag_id,
          dag_run_id: dag_run.id,
          dag_config: dag_config,
          started_at: System.system_time(:millisecond),
          completed_nodes: [],
          running_nodes: %{},
          failed_nodes: [],
          node_results: %{}
        }

        Logger.info("Started DAG run #{run_id} for DAG #{dag_id}")
        {:ok, dag_run_info}

      error ->
        Logger.error("Failed to create DAG run record: #{inspect(error)}")
        {:error, :database_error}
    end
  end

  defp execute_dag_step(state, run_id, dag_run_info) do
    # Check if DAG is complete
    cond do
      dag_complete?(dag_run_info) ->
        complete_dag_run_and_cleanup(state, run_id, dag_run_info)

      dag_failed?(dag_run_info) ->
        fail_dag_run_and_cleanup(state, run_id, dag_run_info)

      true ->
        # Find ready nodes and start them
        start_ready_nodes(state, run_id, dag_run_info)
    end
  end

  defp dag_complete?(dag_run_info) do
    total_nodes = map_size(dag_run_info.dag_config.nodes)
    completed_count = length(dag_run_info.completed_nodes)
    running_count = map_size(dag_run_info.running_nodes)

    completed_count + running_count == total_nodes and running_count == 0
  end

  defp dag_failed?(dag_run_info) do
    # DAG fails if there are failed nodes and no nodes can proceed
    length(dag_run_info.failed_nodes) > 0 and
      get_ready_nodes(dag_run_info) == [] and
      map_size(dag_run_info.running_nodes) == 0
  end

  defp start_ready_nodes(state, run_id, dag_run_info) do
    ready_nodes = get_ready_nodes(dag_run_info)
    max_concurrent = dag_run_info.dag_config.max_concurrent_nodes
    current_running = map_size(dag_run_info.running_nodes)
    available_slots = max(0, max_concurrent - current_running)

    nodes_to_start = Enum.take(ready_nodes, available_slots)

    updated_dag_run_info = Enum.reduce(nodes_to_start, dag_run_info, fn node_id, acc ->
      case start_dag_node(state, run_id, node_id, acc) do
        {:ok, job_ref} ->
          Map.update!(acc, :running_nodes, &Map.put(&1, node_id, job_ref))

        {:error, reason} ->
          Logger.error("Failed to start node #{node_id}: #{inspect(reason)}")
          Map.update!(acc, :failed_nodes, &[node_id | &1])
      end
    end)

    Map.put(state.running_dag_runs, run_id, updated_dag_run_info)
    |> then(&%{state | running_dag_runs: &1})
  end

  defp get_ready_nodes(dag_run_info) do
    completed_set = MapSet.new(dag_run_info.completed_nodes)
    running_set = MapSet.new(Map.keys(dag_run_info.running_nodes))
    failed_set = MapSet.new(dag_run_info.failed_nodes)

    dag_run_info.dag_config.nodes
    |> Enum.filter(fn {node_id, node_config} ->
      # Node is ready if:
      # 1. Not already completed, running, or failed
      # 2. All dependencies are completed
      # 3. Condition is met (if any)
      not MapSet.member?(completed_set, node_id) and
      not MapSet.member?(running_set, node_id) and
      not MapSet.member?(failed_set, node_id) and
      dependencies_satisfied?(node_config, completed_set) and
      condition_satisfied?(node_config, dag_run_info.node_results)
    end)
    |> Enum.map(fn {node_id, _} -> node_id end)
  end

  defp dependencies_satisfied?(node_config, completed_set) do
    depends_on = node_config[:depends_on] || []
    Enum.all?(depends_on, &MapSet.member?(completed_set, &1))
  end

  defp condition_satisfied?(node_config, node_results) do
    case {node_config[:condition_module], node_config[:condition_function]} do
      {nil, nil} -> true
      {module_name, function_name} when is_binary(module_name) and is_binary(function_name) ->
        try do
          module = String.to_existing_atom("Elixir.#{module_name}")
          function = String.to_existing_atom(function_name)
          apply(module, function, [node_results])
        rescue
          error ->
            Logger.error("Condition check failed: #{inspect(error)}")
            false
        end
      _ -> true
    end
  end

  defp start_dag_node(state, run_id, node_id, dag_run_info) do
    node_config = Map.get(dag_run_info.dag_config.nodes, node_id)
    job_id = node_config.job_id

    # Start the job through the regular orchestrator
    case Orchestrator.run_job(state.orchestrator_pid, job_id) do
      {:ok, job_ref} ->
        # Monitor the job completion
        spawn_link(fn ->
          monitor_dag_node(run_id, node_id, job_ref, self())
        end)

        Logger.info("Started node #{node_id} (job #{job_id}) in DAG run #{run_id}")
        {:ok, job_ref}

      error ->
        error
    end
  end

  defp monitor_dag_node(run_id, node_id, job_ref, dag_orchestrator_pid) do
    # This function would monitor the job completion
    # In practice, you'd integrate with your existing job monitoring
    # For now, we'll simulate with a simple approach
    receive do
      {:job_completed, ^job_ref, result} ->
        send(dag_orchestrator_pid, {:node_completed, run_id, node_id, result})

      {:job_failed, ^job_ref, error} ->
        send(dag_orchestrator_pid, {:node_failed, run_id, node_id, error})
    after
      :timer.minutes(30) ->  # Timeout
        send(dag_orchestrator_pid, {:node_failed, run_id, node_id, :timeout})
    end
  end

  defp complete_dag_run_and_cleanup(state, run_id, dag_run_info) do
    Logger.info("DAG run #{run_id} completed successfully")

    # Update database
    complete_dag_run(dag_run_info.dag_run_id, :completed, %{
      completed_nodes: dag_run_info.completed_nodes,
      node_results: dag_run_info.node_results
    })

    # Update last run time
    current_time = System.system_time(:millisecond)
    updated_dag_config = Map.put(dag_run_info.dag_config, :last_run, current_time)
    new_dags = Map.put(state.dags, dag_run_info.dag_id, updated_dag_config)
    update_dag_last_run(dag_run_info.dag_id, current_time)

    # Remove from running DAGs
    new_running_dags = Map.delete(state.running_dag_runs, run_id)

    %{state | running_dag_runs: new_running_dags, dags: new_dags}
  end

  defp fail_dag_run_and_cleanup(state, run_id, dag_run_info) do
    Logger.error("DAG run #{run_id} failed")

    # Update database
    complete_dag_run(dag_run_info.dag_run_id, :failed, %{
      completed_nodes: dag_run_info.completed_nodes,
      failed_nodes: dag_run_info.failed_nodes,
      node_results: dag_run_info.node_results
    })

    # Remove from running DAGs
    new_running_dags = Map.delete(state.running_dag_runs, run_id)

    %{state | running_dag_runs: new_running_dags}
  end

  defp complete_dag_run(dag_run_id, status, results) do
    try do
      dag_run = Repo.get!(DagRun, dag_run_id)

      attrs = %{
        status: status,
        completed_at: DateTime.utc_now(),
        node_statuses: convert_to_status_map(results),
        node_results: results[:node_results] || %{}
      }

      dag_run
      |> DagRun.complete_changeset(attrs)
      |> Repo.update()
    rescue
      error ->
        Logger.error("Failed to complete DAG run #{dag_run_id}: #{inspect(error)}")
    end
  end

  defp convert_to_status_map(results) do
    completed = results[:completed_nodes] || []
    failed = results[:failed_nodes] || []

    completed_map = Enum.into(completed, %{}, fn node -> {node, "completed"} end)
    failed_map = Enum.into(failed, %{}, fn node -> {node, "failed"} end)

    Map.merge(completed_map, failed_map)
  end

  defp find_running_dag_by_dag_id(running_dag_runs, dag_id) do
    Enum.find(running_dag_runs, fn {_run_id, dag_run_info} ->
      dag_run_info.dag_id == dag_id
    end)
  end

  defp is_dag_running?(running_dag_runs, dag_id) do
    find_running_dag_by_dag_id(running_dag_runs, dag_id) != nil
  end

  defp should_run_dag?(dag_config) when is_map(dag_config) do
    case Map.get(dag_config, :schedule) do
      {:interval, interval_ms} ->
        case Map.get(dag_config, :last_run) do
          nil -> true
          last_run -> System.system_time(:millisecond) - last_run >= interval_ms
        end

      :manual -> false
      _ -> false
    end
  end

  defp generate_run_id(dag_id) do
    timestamp = System.system_time(:millisecond)
    "#{dag_id}_#{timestamp}"
  end

  defp schedule_next_check(state) do
    if state.schedule_timer do
      Process.cancel_timer(state.schedule_timer)
    end

    # Check for scheduled DAGs every 2 minutes
    timer = Process.send_after(self(), :check_scheduled_dags, :timer.minutes(2))
    %{state | schedule_timer: timer}
  end

  defp load_persisted_dags(state) do
    try do
      # Load DAG definitions and their nodes
      dag_definitions = from(dd in DagDefinition, where: dd.active == true)
      |> Repo.all()

      persisted_dags = dag_definitions
      |> Enum.reduce(%{}, fn dag_def, acc ->
        case load_dag_nodes(dag_def) do
          {:ok, dag_config} ->
            dag_id = String.to_existing_atom(dag_def.dag_id)
            Map.put(acc, dag_id, dag_config)

          {:error, reason} ->
            Logger.warning("Failed to load DAG definition #{dag_def.dag_id}: #{inspect(reason)}")
            acc
        end
      end)

      %{state | dags: persisted_dags}
    rescue
      error ->
        Logger.warning("Failed to load persisted DAGs: #{inspect(error)}")
        state
    end
  end

  defp load_dag_nodes(dag_def) do
    try do
      nodes_query = from(dn in DagNode, where: dn.dag_id == ^dag_def.dag_id)
      dag_nodes = Repo.all(nodes_query)

      {:ok, base_config} = DagDefinition.to_config(dag_def)

      nodes_map = dag_nodes
      |> Enum.reduce(%{}, fn node, acc ->
        node_id = String.to_existing_atom(node.node_id)
        job_id = String.to_existing_atom(node.job_id)

        node_config = %{
          job_id: job_id,
          depends_on: Enum.map(node.depends_on, &String.to_existing_atom/1),
          condition_module: node.condition_module,
          condition_function: node.condition_function
        }

        Map.put(acc, node_id, node_config)
      end)

      final_config = Map.put(base_config, :nodes, nodes_map)
      {:ok, final_config}
    rescue
      error ->
        {:error, error}
    end
  end

  defp persist_dag_definition(dag_id, dag_config) do
    Repo.transaction(fn ->
      # Insert/update DAG definition
      dag_attrs = DagDefinition.from_config(dag_id, dag_config)

      dag_result = %DagDefinition{}
      |> DagDefinition.changeset(dag_attrs)
      |> Repo.insert(on_conflict: :replace_all, conflict_target: :dag_id)

      case dag_result do
        {:ok, _dag_def} ->
          # Delete existing nodes
          from(dn in DagNode, where: dn.dag_id == ^to_string(dag_id))
          |> Repo.delete_all()

          # Insert new nodes
          dag_config.nodes
          |> Enum.each(fn {node_id, node_config} ->
            node_attrs = %{
              node_id: to_string(node_id),
              dag_id: to_string(dag_id),
              job_id: to_string(node_config.job_id),
              depends_on: Enum.map(node_config[:depends_on] || [], &to_string/1),
              condition_module: node_config[:condition_module],
              condition_function: node_config[:condition_function],
              node_config: inspect(node_config)
            }

            %DagNode{}
            |> DagNode.changeset(node_attrs)
            |> Repo.insert!()
          end)

          {:ok, :persisted}

        {:error, reason} ->
          Repo.rollback(reason)
      end
    end)
  end

  defp update_dag_last_run(dag_id, last_run_time) do
    try do
      from(dd in DagDefinition, where: dd.dag_id == ^to_string(dag_id))
      |> Repo.update_all(set: [last_run: last_run_time])
    rescue
      error ->
        Logger.warning("Failed to update last run time for DAG #{dag_id}: #{inspect(error)}")
    end
  end

  defp get_latest_dag_run(dag_id) do
    dag_id_str = to_string(dag_id)

    DagRun
    |> where([dr], dr.dag_id == ^dag_id_str)
    |> order_by([dr], desc: dr.started_at)
    |> limit(1)
    |> Repo.one()
  end

  defp list_dag_runs(dag_id, opts) do
    limit = Keyword.get(opts, :limit, 50)
    dag_id_str = to_string(dag_id)

    DagRun
    |> where([dr], dr.dag_id == ^dag_id_str)
    |> order_by([dr], desc: dr.started_at)
    |> limit(^limit)
    |> Repo.all()
  end

  @impl true
  def terminate(reason, state) do
    Logger.info("DAG Orchestrator terminating: #{inspect(reason)}")

    if state.schedule_timer do
      Process.cancel_timer(state.schedule_timer)
    end

    # Cancel all running DAGs
    Enum.each(state.running_dag_runs, fn {run_id, dag_run_info} ->
      Enum.each(dag_run_info.running_nodes, fn {_node_id, job_ref} ->
        Orchestrator.cancel_job(state.orchestrator_pid, job_ref)
      end)

      complete_dag_run(dag_run_info.dag_run_id, :cancelled, %{
        cancelled_reason: "DAG Orchestrator shutdown"
      })

      Logger.info("Cancelled DAG run #{run_id} due to shutdown")
    end)

    :ok
  end
end
