defmodule Balsam.SimpleOrchestrator do
  use GenServer
  require Logger

  @moduledoc """
  Simple orchestrator without database persistence.
  Good for testing and basic job management.
  """

  defstruct [
    :jobs,
    :running_jobs,
    :supervisor_pid,
    :max_concurrent_jobs
  ]

  ## Client API

  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  def register_job(orchestrator \\ __MODULE__, job_id, job_config) do
    GenServer.call(orchestrator, {:register_job, job_id, job_config})
  end

  def run_job(orchestrator \\ __MODULE__, job_id) do
    GenServer.call(orchestrator, {:run_job, job_id}, :infinity)
  end

  def get_status(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :get_status)
  end

  def list_running_jobs(orchestrator \\ __MODULE__) do
    GenServer.call(orchestrator, :list_running_jobs)
  end

  ## Server Implementation

  @impl true
  def init(opts) do
    {:ok, supervisor_pid} = Task.Supervisor.start_link()

    state = %__MODULE__{
      jobs: %{},
      running_jobs: %{},
      supervisor_pid: supervisor_pid,
      max_concurrent_jobs: Keyword.get(opts, :max_concurrent_jobs, 3)
    }

    Logger.info("Simple Orchestrator started with max #{state.max_concurrent_jobs} concurrent jobs")
    {:ok, state}
  end

  @impl true
  def handle_call({:register_job, job_id, job_config}, _from, state) do
    validated_config = Map.merge(%{
      args: [],
      enable_progress: false
    }, job_config)

    updated_jobs = Map.put(state.jobs, job_id, validated_config)
    Logger.info("Registered job: #{job_id}")

    {:reply, :ok, %{state | jobs: updated_jobs}}
  end

  @impl true
  def handle_call({:run_job, job_id}, _from, state) do
    cond do
      not Map.has_key?(state.jobs, job_id) ->
        {:reply, {:error, :job_not_found}, state}

      Map.has_key?(state.running_jobs, job_id) ->
        {:reply, {:error, :job_already_running}, state}

      map_size(state.running_jobs) >= state.max_concurrent_jobs ->
        {:reply, {:error, :max_concurrent_jobs_reached}, state}

      true ->
        job_config = Map.get(state.jobs, job_id)

        case start_simple_job(state.supervisor_pid, job_id, job_config) do
          {:ok, task_pid, task_ref} ->
            job_info = %{
              task_pid: task_pid,
              task_ref: task_ref,
              started_at: System.system_time(:millisecond),
              status: :running
            }

            new_running_jobs = Map.put(state.running_jobs, job_id, job_info)
            Logger.info("Started job: #{job_id}")

            {:reply, {:ok, task_ref}, %{state | running_jobs: new_running_jobs}}

          error ->
            Logger.error("Failed to start job #{job_id}: #{inspect(error)}")
            {:reply, error, state}
        end
    end
  end

  @impl true
  def handle_call(:get_status, _from, state) do
    status = %{
      registered_jobs: Map.keys(state.jobs),
      running_jobs: Map.keys(state.running_jobs),
      running_count: map_size(state.running_jobs),
      max_concurrent: state.max_concurrent_jobs,
      available_slots: state.max_concurrent_jobs - map_size(state.running_jobs)
    }
    {:reply, status, state}
  end

  @impl true
  def handle_call(:list_running_jobs, _from, state) do
    running_jobs = state.running_jobs
    |> Enum.map(fn {job_id, job_info} ->
      {job_id, %{
        status: job_info.status,
        started_at: job_info.started_at,
        running_time_ms: System.system_time(:millisecond) - job_info.started_at
      }}
    end)
    |> Enum.into(%{})

    {:reply, running_jobs, state}
  end

  ## Handle job completion
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case find_job_by_ref(state.running_jobs, ref) do
      {job_id, _job_info} ->
        case reason do
          :normal ->
            Logger.info("Job #{job_id} completed successfully")

          error ->
            Logger.error("Job #{job_id} failed: #{inspect(error)}")
        end

        # Remove from running jobs
        new_running_jobs = Map.delete(state.running_jobs, job_id)

        {:noreply, %{state | running_jobs: new_running_jobs}}

      nil ->
        {:noreply, state}
    end
  end

  ## Private Functions

  defp start_simple_job(supervisor_pid, job_id, job_config) do
    task_fun = fn ->
      try do
        module = job_config.module
        function = job_config.function
        args = job_config.args || []

        Logger.info("Executing #{module}.#{function}")
        result = apply(module, function, args)
        Logger.info("Job #{job_id} completed successfully")
        {:ok, result}
      rescue
        error ->
          Logger.error("Job #{job_id} failed: #{inspect(error)}")
          {:error, error}
      end
    end

    case Task.Supervisor.start_child(supervisor_pid, task_fun) do
      {:ok, task_pid} ->
        task_ref = Process.monitor(task_pid)
        {:ok, task_pid, task_ref}

      error ->
        error
    end
  end

  defp find_job_by_ref(running_jobs, ref) do
    Enum.find(running_jobs, fn {_job_id, job_info} ->
      job_info.task_ref == ref
    end)
  end
end
