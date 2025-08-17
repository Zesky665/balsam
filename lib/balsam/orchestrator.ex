defmodule Balsam.Orchestrator do
  use GenServer
  require Logger
  alias Balsam.{JobRunner, DagRunner, WorkflowRegistry}

  @moduledoc """
  Main orchestrator that coordinates job and DAG execution.
  Delegates actual execution to specialized modules.
  """

  defstruct [
    :workflows,
    :running_jobs,
    :running_dags,
    :max_concurrent
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def register_workflow(orchestrator \\ __MODULE__, workflow_id, workflow_config) do
    GenServer.call(orchestrator, {:register_workflow, workflow_id, workflow_config})
  end

  def run_job(orchestrator \\ __MODULE__, job_id, progress_callback \\ nil) do
    GenServer.call(orchestrator, {:run_job, job_id, progress_callback}, :infinity)
  end

  def run_dag(orchestrator \\ __MODULE__, dag_id, run_id \\ nil) do
    GenServer.call(orchestrator, {:run_dag, dag_id, run_id}, :infinity)
  end

  def cancel_job(orchestrator \\ __MODULE__, job_ref) do
    GenServer.call(orchestrator, {:cancel_job, job_ref})
  end

  def cancel_dag(orchestrator \\ __MODULE__, dag_id) do
    GenServer.call(orchestrator, {:cancel_dag, dag_id})
  end

  def get_job_status(orchestrator \\ __MODULE__, job_ref) do
    GenServer.call(orchestrator, {:get_job_status, job_ref})
  end

  def get_dag_status(orchestrator \\ __MODULE__, dag_id) do
    GenServer.call(orchestrator, {:get_dag_status, dag_id})
  end

  def get_status(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :get_status)
  end

  ## Server Implementation

  @impl true
  def init(opts) do
    state = %__MODULE__{
      workflows: %{},
      running_jobs: %{},
      running_dags: %{},
      max_concurrent: Keyword.get(opts, :max_concurrent, 5)
    }

    Logger.info("Orchestrator started")
    {:ok, state}
  end

  # @impl true
  # def handle_call({:register_dag, dag_id, dag_config}, _from, state) do
  #   case WorkflowRegistry.validate_workflow_config(dag_id, dag_config) do
  #     :ok ->
  #       new_workflows = Map.put(state.workflows, dag_id, dag_config)
  #       Logger.info("Registered DAG: #{dag_id}")
  #       {:reply, :ok, %{state | workflows: new_workflows}}

  #     {:error, reason} ->
  #       {:reply, {:error, reason}, state}
  #   end
  # end

  @impl true
  def handle_call({:register_workflow, workflow_id, workflow_config}, _from, state) do
    case WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config) do
      :ok ->
        new_workflows = Map.put(state.workflows, workflow_id, workflow_config)
        Logger.info("Registered workflow: #{workflow_id}")
        {:reply, :ok, %{state | workflows: new_workflows}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:run_job, job_id, progress_callback}, _from, state) do
    case can_start_job?(state) do
      false ->
        {:reply, {:error, :max_concurrent_reached}, state}

      true ->
        case Map.get(state.workflows, job_id) do
          nil ->
            {:reply, {:error, :job_not_found}, state}

          workflow_config ->
            case JobRunner.start_job(job_id, workflow_config, progress_callback) do
              {:ok, job_ref} ->
                new_running = Map.put(state.running_jobs, job_ref, %{
                  job_id: job_id,
                  started_at: System.system_time(:millisecond)
                })
                {:reply, {:ok, job_ref}, %{state | running_jobs: new_running}}

              error ->
                {:reply, error, state}
            end
        end
    end
  end

  @impl true
  def handle_call({:run_dag, dag_id, run_id}, _from, state) do
    case can_start_dag?(state) do
      false ->
        {:reply, {:error, :max_concurrent_reached}, state}

      true ->
        case Map.get(state.workflows, dag_id) do
          nil ->
            {:reply, {:error, :dag_not_found}, state}

          workflow_config ->
            case DagRunner.start_dag(dag_id, workflow_config, run_id) do
              {:ok, dag_run_id} ->
                new_running = Map.put(state.running_dags, dag_run_id, %{
                  dag_id: dag_id,
                  started_at: System.system_time(:millisecond)
                })
                {:reply, {:ok, dag_run_id}, %{state | running_dags: new_running}}

              error ->
                {:reply, error, state}
            end
        end
    end
  end

  @impl true
  def handle_call({:cancel_job, job_ref}, _from, state) do
    case JobRunner.cancel_job(job_ref) do
      :ok ->
        new_running = Map.delete(state.running_jobs, job_ref)
        {:reply, :ok, %{state | running_jobs: new_running}}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:cancel_dag, dag_id}, _from, state) do
    case find_running_dag(state.running_dags, dag_id) do
      nil ->
        {:reply, {:error, :dag_not_running}, state}

      {dag_run_id, _info} ->
        case DagRunner.cancel_dag(dag_run_id) do
          :ok ->
            new_running = Map.delete(state.running_dags, dag_run_id)
            {:reply, :ok, %{state | running_dags: new_running}}

          error ->
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call({:get_job_status, job_ref}, _from, state) do
    status = JobRunner.get_job_status(job_ref)
    {:reply, status, state}
  end

  @impl true
  def handle_call({:get_dag_status, dag_id}, _from, state) do
    status = DagRunner.get_dag_status(dag_id)
    {:reply, status, state}
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      registered_workflows: Map.keys(state.workflows),
      running_jobs: map_size(state.running_jobs),
      running_dags: map_size(state.running_dags),
      max_concurrent: state.max_concurrent
    }
    {:reply, status, state}
  end

  @impl true
  def handle_info({:job_completed, job_ref}, state) do
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  @impl true
  def handle_info({:job_failed, job_ref, _reason}, state) do
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  @impl true
  def handle_info({:dag_completed, dag_run_id}, state) do
    new_running = Map.delete(state.running_dags, dag_run_id)
    {:noreply, %{state | running_dags: new_running}}
  end

  @impl true
  def handle_info({:dag_failed, dag_run_id, _reason}, state) do
    new_running = Map.delete(state.running_dags, dag_run_id)
    {:noreply, %{state | running_dags: new_running}}
  end

  ## Private Functions

  defp can_start_job?(state) do
    total_running = map_size(state.running_jobs) + map_size(state.running_dags)
    total_running < state.max_concurrent
  end

  defp can_start_dag?(state) do
    can_start_job?(state)
  end

  defp find_running_dag(running_dags, dag_id) do
    Enum.find(running_dags, fn {_run_id, info} ->
      info.dag_id == dag_id
    end)
  end
end
