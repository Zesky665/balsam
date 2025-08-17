defmodule Balsam.Orchestrator do
  use GenServer
  require Logger
  alias Balsam.{JobRunner, DagRunner, WorkflowRegistry, JobRegistry}

  @moduledoc """
  Fault-tolerant orchestrator that survives job failures and maintains system stability.
  Uses supervision trees and defensive programming to ensure resilience.
  """

  defstruct [
    :workflows,
    :running_jobs,
    :running_dags,
    :max_concurrent,
    :failed_jobs,
    :retry_policies
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

  def retry_job(orchestrator \\ __MODULE__, job_ref) do
    GenServer.call(orchestrator, {:retry_job, job_ref})
  end

  def get_job_status(orchestrator \\ __MODULE__, job_ref) do
    GenServer.call(orchestrator, {:get_job_status, job_ref})
  end

  def get_status(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :get_status)
  end

  ## Server Implementation

  @impl true
  def init(opts) do
    # Set process flag to trap exits for proper cleanup
    Process.flag(:trap_exit, true)

    state = %__MODULE__{
      workflows: %{},
      running_jobs: %{},
      running_dags: %{},
      failed_jobs: %{},
      retry_policies: %{},
      max_concurrent: Keyword.get(opts, :max_concurrent, 5)
    }

    Logger.info("Fault-tolerant orchestrator started")
    {:ok, state}
  end

  @impl true
  def handle_call({:register_workflow, workflow_id, workflow_config}, _from, state) do
    case safely_validate_workflow(workflow_id, workflow_config) do
      :ok ->
        new_workflows = Map.put(state.workflows, workflow_id, workflow_config)

        # Set up retry policy for this workflow
        retry_policy = extract_retry_policy(workflow_config)
        new_retry_policies = Map.put(state.retry_policies, workflow_id, retry_policy)

        Logger.info("Registered workflow: #{workflow_id}")
        {:reply, :ok, %{state |
          workflows: new_workflows,
          retry_policies: new_retry_policies
        }}

      {:error, reason} ->
        Logger.warning("Failed to register workflow #{workflow_id}: #{reason}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:run_job, job_id, progress_callback}, _from, state) do
    case validate_job_execution(job_id, state) do
      :ok ->
        case safely_start_job(job_id, state, progress_callback) do
          {:ok, job_ref, job_info} ->
            new_running = Map.put(state.running_jobs, job_ref, job_info)
            {:reply, {:ok, job_ref}, %{state | running_jobs: new_running}}

          {:error, reason} ->
            Logger.error("Failed to start job #{job_id}: #{inspect(reason)}")
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:cancel_job, job_ref}, _from, state) do
    case Map.get(state.running_jobs, job_ref) do
      nil ->
        {:reply, {:error, :job_not_found}, state}

      _job_info ->
        case safely_cancel_job(job_ref) do
          :ok ->
            new_running = Map.delete(state.running_jobs, job_ref)
            {:reply, :ok, %{state | running_jobs: new_running}}

          error ->
            Logger.warning("Failed to cancel job #{inspect(job_ref)}: #{inspect(error)}")
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call({:retry_job, job_ref}, _from, state) do
    case Map.get(state.failed_jobs, job_ref) do
      nil ->
        {:reply, {:error, :job_not_found_in_failed}, state}

      failed_job_info ->
        case can_retry_job?(failed_job_info, state) do
          true ->
            case retry_failed_job(failed_job_info, state) do
              {:ok, new_job_ref} ->
                new_failed = Map.delete(state.failed_jobs, job_ref)
                {:reply, {:ok, new_job_ref}, %{state | failed_jobs: new_failed}}

              error ->
                {:reply, error, state}
            end

          false ->
            {:reply, {:error, :max_retries_exceeded}, state}
        end
    end
  end

  @impl true
  def handle_call({:get_job_status, job_ref}, _from, state) do
    cond do
      Map.has_key?(state.running_jobs, job_ref) ->
        job_info = Map.get(state.running_jobs, job_ref)
        status = %{
          status: :running,
          job_ref: job_ref,
          job_id: job_info.job_id,
          started_at: job_info.started_at,
          runtime: System.system_time(:millisecond) - job_info.started_at
        }
        {:reply, {:ok, status}, state}

      Map.has_key?(state.failed_jobs, job_ref) ->
        job_info = Map.get(state.failed_jobs, job_ref)
        status = %{
          status: :failed,
          job_ref: job_ref,
          job_id: job_info.job_id,
          error: job_info.error,
          retry_count: job_info.retry_count,
          can_retry: can_retry_job?(job_info, state)
        }
        {:reply, {:ok, status}, state}

      true ->
        {:reply, {:error, :job_not_found}, state}
    end
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      registered_workflows: Map.keys(state.workflows),
      running_jobs: map_size(state.running_jobs),
      failed_jobs: map_size(state.failed_jobs),
      running_dags: map_size(state.running_dags),
      max_concurrent: state.max_concurrent,
      system_health: calculate_system_health(state)
    }
    {:reply, status, state}
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
            case Balsam.DagRunner.start_dag(dag_id, workflow_config, run_id) do
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

  # Helper function
  defp can_start_dag?(state) do
    total_running = map_size(state.running_jobs) + map_size(state.running_dags)
    total_running < state.max_concurrent
  end

  # Handle job completion messages
  @impl true
  def handle_info({{:job_completed, _pid}, job_ref}, state) do
    Logger.info("Job #{inspect(job_ref)} completed successfully")
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  @impl true
  def handle_info({{:job_failed, _pid, reason}, job_ref}, state) do
    case Map.get(state.running_jobs, job_ref) do
      nil ->
        Logger.warning("Received failure notification for unknown job #{inspect(job_ref)}")
        {:noreply, state}

      job_info ->
        Logger.warning("Job #{job_info.job_id} failed: #{inspect(reason)}")

        # Move job from running to failed
        failed_job_info = Map.merge(job_info, %{
          error: reason,
          failed_at: System.system_time(:millisecond),
          retry_count: Map.get(job_info, :retry_count, 0)
        })

        new_running = Map.delete(state.running_jobs, job_ref)
        new_failed = Map.put(state.failed_jobs, job_ref, failed_job_info)

        # Consider automatic retry if policy allows
        new_state = %{state | running_jobs: new_running, failed_jobs: new_failed}
        maybe_auto_retry(job_ref, failed_job_info, new_state)
    end
  end

  @impl true
  def handle_info({{:job_cancelled, _pid}, job_ref}, state) do
    Logger.info("Job  #{inspect(job_ref)} was cancelled")
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  # Handle process exits gracefully
  @impl true
  def handle_info({:EXIT, _pid, reason}, state) do
    Logger.info("Child process exited: #{inspect(reason)}")
    {:noreply, state}
  end

  # Handle job completion messages (add these to your orchestrator)
  @impl true
  def handle_info({:job_completed, job_ref}, state) do
    Logger.info("Job  #{inspect(job_ref)} completed successfully")
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  @impl true
  def handle_info({:job_failed, job_ref, reason}, state) do
    case Map.get(state.running_jobs, job_ref) do
      nil ->
        Logger.warning("Received failure notification for unknown job  #{inspect(job_ref)}")
        {:noreply, state}

      job_info ->
        Logger.warning("Job #{job_info.job_id} failed: #{inspect(reason)}")

        # Move job from running to failed
        failed_job_info = Map.merge(job_info, %{
          error: reason,
          failed_at: System.system_time(:millisecond),
          retry_count: Map.get(job_info, :retry_count, 0)
        })

        new_running = Map.delete(state.running_jobs, job_ref)
        new_failed = Map.put(state.failed_jobs || %{}, job_ref, failed_job_info)

        {:noreply, %{state | running_jobs: new_running, failed_jobs: new_failed}}
    end
  end

  @impl true
  def handle_info({:job_cancelled, job_ref}, state) do
    Logger.info("Job  #{inspect(job_ref)} was cancelled")
    new_running = Map.delete(state.running_jobs, job_ref)
    {:noreply, %{state | running_jobs: new_running}}
  end

  # Handle process monitoring messages
  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, reason}, state) do
    Logger.debug("Monitored process exited: #{inspect(reason)}")
    {:noreply, state}
  end

  # Handle DAG completion messages (if you implement DAGs later)
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

  # Catch-all for unexpected messages (prevents crashes)
  @impl true
  def handle_info(msg, state) do
    Logger.warning("Received unexpected message: #{inspect(msg)}")
    {:noreply, state}
  end

  ## Private Functions

  defp safely_validate_workflow(workflow_id, workflow_config) do
    try do
      WorkflowRegistry.validate_workflow_config(workflow_id, workflow_config)
    rescue
      error ->
        Logger.error("Validation failed for workflow #{workflow_id}: #{inspect(error)}")
        {:error, "Validation error: #{inspect(error)}"}
    end
  end

  defp validate_job_execution(job_id, state) do
    cond do
      not Map.has_key?(state.workflows, job_id) ->
        {:error, :job_not_found}

      not can_start_new_job?(state) ->
        {:error, :max_concurrent_reached}

      true ->
        :ok
    end
  end

  defp safely_start_job(job_id, state, progress_callback) do
    try do
      workflow_config = Map.get(state.workflows, job_id)

      case JobRunner.start_job(job_id, workflow_config, progress_callback) do
        {:ok, job_ref} ->
          job_info = %{
            job_id: job_id,
            started_at: System.system_time(:millisecond),
            retry_count: 0,
            progress_callback: progress_callback
          }

          # Register the job for tracking
          JobRegistry.register_job(job_ref, self())

          {:ok, job_ref, job_info}

        error ->
          error
      end
    rescue
      error ->
        Logger.error("Failed to start job #{job_id}: #{inspect(error)}")
        {:error, {:startup_error, error}}
    end
  end

  defp safely_cancel_job(job_ref) do
    try do
      JobRunner.cancel_job(job_ref)
    rescue
      error ->
        Logger.error("Error cancelling job  #{inspect(job_ref)}: #{inspect(error)}")
        {:error, {:cancel_error, error}}
    end
  end

  defp can_start_new_job?(state) do
    total_running = map_size(state.running_jobs) + map_size(state.running_dags)
    total_running < state.max_concurrent
  end

  defp extract_retry_policy(workflow_config) do
    %{
      max_retries: workflow_config[:max_retries] || 3,
      retry_delay: workflow_config[:retry_delay] || :timer.seconds(30),
      auto_retry: workflow_config[:auto_retry] || false
    }
  end

  defp can_retry_job?(failed_job_info, state) do
    retry_policy = Map.get(state.retry_policies, failed_job_info.job_id, %{max_retries: 3})
    failed_job_info.retry_count < retry_policy.max_retries
  end

  defp maybe_auto_retry(job_ref, failed_job_info, state) do
    retry_policy = Map.get(state.retry_policies, failed_job_info.job_id, %{auto_retry: false})

    if retry_policy.auto_retry and can_retry_job?(failed_job_info, state) do
      Logger.info("Auto-retrying job #{failed_job_info.job_id} in #{retry_policy.retry_delay}ms")
      Process.send_after(self(), {:auto_retry, job_ref}, retry_policy.retry_delay)
    end

    {:noreply, state}
  end

  defp retry_failed_job(failed_job_info, state) do
    Logger.info("Retrying job #{failed_job_info.job_id} (attempt #{failed_job_info.retry_count + 1})")

    case safely_start_job(failed_job_info.job_id, state, failed_job_info.progress_callback) do
      {:ok, new_job_ref, job_info} ->
        # Update retry count
        updated_job_info = Map.put(job_info, :retry_count, failed_job_info.retry_count + 1)
        new_running = Map.put(state.running_jobs, new_job_ref, updated_job_info)

        {:ok, new_job_ref}

      error ->
        error
    end
  end

  defp calculate_system_health(state) do
    total_jobs = map_size(state.running_jobs) + map_size(state.failed_jobs)

    if total_jobs == 0 do
      :healthy
    else
      failure_rate = map_size(state.failed_jobs) / total_jobs
      cond do
        failure_rate < 0.1 -> :healthy
        failure_rate < 0.3 -> :degraded
        true -> :unhealthy
      end
    end
  end
end
